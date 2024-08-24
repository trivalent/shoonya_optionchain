import concurrent.futures
import requests
import pandas as pd
from zipfile import ZipFile
from io import BytesIO

from PyQt6 import QtCore
from PyQt6.QtCore import pyqtSlot
from PyQt6.QtWidgets import QLabel, QPushButton, QListWidget, QHBoxLayout, QDialog, QInputDialog, \
    QListWidgetItem, QVBoxLayout, QTableView, QComboBox, QHeaderView, QAbstractItemView

from api_helper import ShoonyaApiPy
from ShoonyaWebsocket import ShoonyaWebSocket
import os
import logging
import yaml

#enable dbug to see request and responses
logging.basicConfig(level=logging.DEBUG)

from table_model import PandasTableModel


class TaskManager(QtCore.QObject):
    finished = QtCore.pyqtSignal(object)

    def __init__(self, parent=None, max_workers=None):
        super().__init__(parent)
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)

    @property
    def executor(self):
        return self._executor

    def submit(self, fn, *args, **kwargs):
        future = self.executor.submit(fn, *args, **kwargs)
        future.add_done_callback(self._internal_done_callback)

    def _internal_done_callback(self, future):
        data = future.result()
        self.finished.emit(data)


class ShoonyaWindow(QDialog):
    def __init__(self, parent=None):
        super(ShoonyaWindow, self).__init__(parent)
        self.cred = None
        #load the credentials file first, we won't continue without that
        try:
            with open('cred.yml') as f:
                self.cred = yaml.load(f, Loader=yaml.FullLoader)
        except:
            raise FileNotFoundError("Can't find or load credential file. Please ensure you have a valid cred.yml file")

        # set root certificate path in case you are behind ZScalar or similar enterprise network
        if self.cred['ca_bundle_path'] is not '':
            os.environ['REQUESTS_CA_BUNDLE'] = self.cred['ca_bundle_path']

        # create the downloader task to download and read fno master file
        task_manager = TaskManager()
        task_manager.submit(self._read_fno_master)
        task_manager.finished.connect(self.on_download_complete)

        # initialize the Shoonya API
        self.shoonyaAPI = ShoonyaApiPy()
        # create the Web socket class
        self.websocket = ShoonyaWebSocket(self.shoonyaAPI)
        self.websocket.onPriceUpdate.connect(self.on_price_update)

        self.isLoggedIn = False
        self.currentChain = None
        self.currentStock = ""
        self.currentSubscription = None
        self.processUpdate = False


        # creating the UI
        self.nameLabel = QLabel("Not Logged In")
        self.loginButton = QPushButton("Login")
        self.loginButton.clicked.connect(self.on_login_clicked)

        self.exitAllPositionButton = QPushButton("Exit All Positions")
        self.exitSelectedPositionButton = QPushButton("Exit Selected Position")
        self.buy = QPushButton("Buy")
        self.sell = QPushButton("Sell")

        self.stockList = QListWidget()
        self.stockList.itemClicked.connect(self.on_stock_selected)

        optionsview_container = QVBoxLayout()
        expiry_layout = QHBoxLayout()
        expiry_label = QLabel("Expiry: ")

        self.expiryCombo = QComboBox()
        self.expiryCombo.currentIndexChanged.connect(self.on_update_expiry_date)
        expiry_layout.addWidget(expiry_label, stretch=0)
        expiry_layout.addWidget(self.expiryCombo, stretch=1)

        self.optionsTable = QTableView()
        self.optionsTable.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.optionsTable.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self.optionsTable.clicked.connect(self._option_selected)


        optionsview_container.addLayout(expiry_layout)
        optionsview_container.addWidget(self.optionsTable)

        hbox_layout = QHBoxLayout()
        hbox_layout.addWidget(self.stockList, stretch=0)
        hbox_layout.addLayout(optionsview_container, stretch=1)

        self.infoLayout = QHBoxLayout()
        self.infoLayout.addWidget(self.nameLabel)
        self.infoLayout.addStretch(1)
        self.infoLayout.addWidget(self.loginButton)

        main_layout = QVBoxLayout()
        main_layout.addLayout(self.infoLayout)
        main_layout.addLayout(hbox_layout)

        self.setLayout(main_layout)

    ### called when login button is clicked
    def on_login_clicked(self):
        if not self.isLoggedIn:
            # ask for 2FA
            totp, ok_pressed = QInputDialog.getText(self, "2FA", "Enter TOTP")
            if ok_pressed and totp != '':
                print(f"Received totp : {totp}")
                # perform login
                ret = self.shoonyaAPI.login(self.cred['user'],
                                            self.cred['password'],
                                            totp,
                                            self.cred['vc'],
                                            self.cred['apikey'],
                                            self.cred['imei'])
                # if login is success, start UI update in case option chain is already selected
                if ret is not None:
                    self.isLoggedIn = True
                    self.nameLabel.setText(ret['uname'])
                    self.loginButton.setText("Logout")
                    self.websocket.start()
                    if self.currentSubscription is not None:
                        self.shoonyaAPI.subscribe(self.currentSubscription)
        else:
            if self.currentSubscription is not None:
                self.shoonyaAPI.unsubscribe(self.currentSubscription)
                self.currentSubscription = None

            self.isLoggedIn = False
            self.websocket.exit()
            self.shoonyaAPI.logout()
            self.nameLabel.setText("Not Logged In")
            self.loginButton.setText("Login")

    def _read_fno_master(self):
        r = requests.get("https://api.shoonya.com/NFO_symbols.txt.zip")
        files = ZipFile(BytesIO(r.content))
        # read the csv file with in the zip
        self.fnoData = pd.read_csv(files.open("NFO_symbols.txt"), parse_dates=[5])

        # we are not interested in any of the NIFTY/BankNifty/FinNifty symbols as of now, so exclude them
        # also, Finvasia packages some TEST symbols in the master data, exclude them as well.
        self.fnoData = self.fnoData[~self.fnoData.Symbol.str.contains("NSETEST")][~self.fnoData.Symbol.str.contains("NIFTY")]

    def on_download_complete(self):
        if self.fnoData is None:
            raise ValueError("Unable to read FnO master data. Can't continue")

        # read the list of stocks
        fno_stock_list = self.fnoData['Symbol'].sort_values(ascending=True).unique()

        # read the expiry dates
        fno_expiries = self.fnoData['Expiry'].sort_values(ascending=True).unique().strftime('%d-%b-%Y')

        # add the list of stocks into the stock list widget
        [self.stockList.addItem(item) for item in [QListWidgetItem(name) for name in fno_stock_list]]
        # add the expiry dates into the combo widget
        [self.expiryCombo.addItem(item) for item in fno_expiries]

    def on_stock_selected(self, item):
        self._update_option_chain(item.text())

    def _update_option_chain(self, current_stock):
        print(f'update option chain for {current_stock}')
        self.processUpdate = False

        # if there are existing subscription and we are logged in, let's unscribe from previous updates.
        if self.currentSubscription is not None and self.isLoggedIn:
            self.shoonyaAPI.unsubscribe(self.currentSubscription)
            self.currentSubscription = None

        # get the option chain for this stock symbol
        stock_options_list = self.fnoData[self.fnoData['Symbol'] == current_stock].sort_values(by=['StrikePrice'], ascending=True)

        # prepare the data frame for the options table view. We are displaying only first 3 columns. The other columns
        # are kept so that we can easily access required cells when there is an update as well as we want to place an order.
        # this is used in self._option_selected when we need to determine the token number and trading symbol of
        # selected option
        df = pd.DataFrame(columns=["CE Price", "Strike", "PE Price", "CE_Token", "CE_TradingSymbol", "PE_Token", "PE_TradingSymbol"], data=[])

        # read the current selected expiry
        expiry_date = self.expiryCombo.currentText()

        # filter the option chain based on the selected expiry date.
        stock_options_list = stock_options_list[stock_options_list['Expiry'] == pd.to_datetime(expiry_date)]

        #separate the option chain for PE and CE
        current_pe_chain = stock_options_list[stock_options_list['OptionType'] == "PE"]
        current_ce_chain = stock_options_list[stock_options_list['OptionType'] == "CE"]

        # it is found that for few scripts, PE and CE option chains have missing strikes. We need to ensure both contains same set of strikes
        if current_pe_chain.shape != current_ce_chain.shape:
            combined_values = pd.Series(list(set(current_pe_chain['StrikePrice']).intersection(current_ce_chain['StrikePrice'])))
            current_ce_chain = current_ce_chain[current_ce_chain['StrikePrice'].isin(combined_values)]
            current_pe_chain = current_pe_chain[current_pe_chain['StrikePrice'].isin(combined_values)]

        # fill the data to be displayed in the option chain table.
        df["Strike"] = current_pe_chain["StrikePrice"].values
        # since we don't have the price information yet, keep it 0
        df["CE Price"] = 0.0
        df["PE Price"] = 0.0
        df["PE_Token"] = current_pe_chain['Token'].values
        df["CE_Token"] = current_ce_chain['Token'].values
        df["CE_TradingSymbol"] = current_ce_chain['TradingSymbol'].values
        df["PE_TradingSymbol"] = current_pe_chain['TradingSymbol'].values

        # save the current option chain data frame
        self.currentChain = df
        self.currentStock = current_stock

        # create table model from the option chain and set it to the options table view
        self.optionsTable.setModel(PandasTableModel(data=self.currentChain))

        # prepare the token list for subscribing to price updates.
        ce_subscription = [f'NFO|{name}' for name in current_ce_chain['Token']]
        pe_subscription = [f'NFO|{name}' for name in current_pe_chain['Token']]

        # save the list
        self.currentSubscription = ce_subscription + pe_subscription

        # if we are already logged in, subscribe for the pricing updates.
        if self.isLoggedIn:
            self.shoonyaAPI.subscribe(self.currentSubscription)

        self.processUpdate = True

    def on_update_expiry_date(self, new_date):
        if self.currentStock != "":
            self._update_option_chain(self.currentStock)

    def _option_selected(self, item):
        option_chain = None
        col = item.column()

        # the column layout is [CE Price | Strike | PE Price]. Based on this we are finding what is clicked in the
        # options table
        if item.column() == 0:
            col = col + 1
            option_chain = "CE"
        elif item.column() == 2:
            col = col - 1
            option_chain = "PE"

        if option_chain is not None:
            strike_price = self.currentChain.iat[item.row(), col]
            token_number = 0
            trading_symbol = ""

            if option_chain == "PE":
                token_number = self.currentChain.iat[item.row(), col + 4]
                trading_symbol = self.currentChain.iat[item.row(), col + 5]
            else:
                token_number = self.currentChain.iat[item.row(), col + 2]
                trading_symbol = self.currentChain.iat[item.row(), col + 3]

            print(f'selected strike price {strike_price} for {option_chain} with Token Number: {token_number} and TradingSymbol = {trading_symbol}')
        else:
            print('User selected strike price, nothing to be done')

    @pyqtSlot(int, str, bool, name="onPriceUpdate")
    def on_price_update(self, token, ltp, is_banned):
        print(f'Price update received for {token} with ltp = {ltp}. is the script in F&O Ban = {is_banned}, process this update = {self.processUpdate}')
        if not self.processUpdate:
            return

        # check if the update received is for PE or CE
        is_ce_token = self.currentChain.index[self.currentChain['CE_Token'] == token].values
        is_pe_token = self.currentChain.index[self.currentChain['PE_Token'] == token].values

        price_col = 0
        index_val = 0
        price_field = 'CE Price'
        # if it is PE or CE symbol, set the Column where Price needs to be updated and the Column name accordingly.
        if is_pe_token.size > 0:
            index_val = is_pe_token[0]
            price_col = 2
            price_field = 'PE Price'
        else:
            index_val = is_ce_token[0]
            price_col = 0

        pandas_model : PandasTableModel = self.optionsTable.model()
        # ask the model to update the price for the said CELL.
        pandas_model.update_price(price_field, price_col, index_val, ltp)
