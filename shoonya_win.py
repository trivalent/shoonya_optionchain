import concurrent.futures
from datetime import datetime
from logging import Logger
from typing import Any

import requests
import pandas as pd
from zipfile import ZipFile
from io import BytesIO

from PySide6 import QtCore
from PySide6.QtCore import Signal, QThread, Slot
from PySide6.QtWidgets import QDialog, QLabel, QPushButton, QListWidget, QVBoxLayout, QHBoxLayout, QComboBox, \
    QTableView, QHeaderView, QAbstractItemView, QInputDialog, QListWidgetItem, QTabWidget

from ShoonyaAPIWrapper import ShoonyaAPIWrapper
from api_helper import ShoonyaApiPy, Order, BuyOrderMarket, SellOrderMarket, BuyOrder
import os
import logging
import yaml

#enable dbug to see request and responses
logging.basicConfig(level=logging.INFO)

from table_model import OptionChainTableModel, QHighlightDelegate, PositionsTableModel


class TaskManager(QtCore.QObject):
    finished = Signal(object)

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
    logger = logging.getLogger(__name__)

    # fired when the UI as collected the TOTP
    on_perform_login = Signal(Any)
    on_perform_logout = Signal()
    on_subscribe_instrument = Signal(list)
    on_unsubscribe_instrument = Signal(list)
    get_positions = Signal()

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
        if self.cred['ca_bundle_path'] != '':
            os.environ['REQUESTS_CA_BUNDLE'] = self.cred['ca_bundle_path']

        # create the downloader task to download and read fno master file
        fno_downloader = TaskManager(self, max_workers=2)
        fno_downloader.submit(self._read_fno_master)
        fno_downloader.finished.connect(self.on_fno_download_complete)

        nse_downloader = TaskManager(self, max_workers=2)
        nse_downloader.submit(self._read_nse_master)
        nse_downloader.finished.connect(self.on_nse_download_complete)

        # initialize the Shoonya API wrapper
        self.shoonyaApiWrapper = ShoonyaAPIWrapper(api=ShoonyaApiPy())
        # create a new thread
        t = QThread(self)
        # move the api wrapper object to thread so that it runs on a separate thread.
        self.shoonyaApiWrapper.moveToThread(t)
        # start the thread
        t.start()

        self._isLoggedIn = False
        self.currentChain = None
        self.lotSize = 0
        self.currentStock = ""
        self.currentSubscription = None
        self.processUpdate = False
        self.buyOrder: Order = None
        self.sellOrder: Order = None
        self.fnoData: pd.DataFrame = None
        self.stockData: pd.DataFrame = None
        self.current_positions: pd.DataFrame = None


        # creating the UI
        self._setup_ui()
        self._setup_signals()
        self._setup_ui_styling()

    def _setup_ui(self):
        self.nameLabel = QLabel("Not Logged In")
        self.loginButton = QPushButton("Login")

        self.exitAllPositionButton = QPushButton("Exit All Positions")
        self.exitSelectedPositionButton = QPushButton("Exit Selected Position")
        self.buy = QPushButton("Buy")
        self.sell = QPushButton("Sell")

        self.fno_stock_list = QListWidget()
        self.nse_stock_list = QListWidget()

        optionsview_container = QVBoxLayout()
        expiry_layout = QHBoxLayout()
        expiry_label = QLabel("Expiry: ")
        order_type = QLabel("Order Type: ")

        self.expiryCombo = QComboBox()

        # order type comobo: MIS is Intra Day and NRML is carry forward
        self.orderCombo = QComboBox()
        self.orderCombo.addItem("NRML")
        self.orderCombo.addItem("MIS")

        expiry_layout.addWidget(expiry_label, stretch=0)
        expiry_layout.addWidget(self.expiryCombo, stretch=1)
        expiry_layout.addWidget(order_type, stretch=0)
        expiry_layout.addWidget(self.orderCombo, stretch=1)

        self.optionsTable = QTableView()
        self.optionsTable.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.optionsTable.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)

        options_table_layout = QVBoxLayout()
        options_table_layout.addWidget(QLabel("Option Chain"))
        options_table_layout.addWidget(self.optionsTable)

        self.bannedWarning = QLabel("This SCRIP is in BAN. Order placing is not allowed")
        self.bannedWarning.setVisible(False)

        self.stocks_fno_positions = QTableView()
        self.stocks_fno_positions.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.stocks_fno_positions.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)

        self.index_fno_positions = QTableView()
        self.index_fno_positions.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.index_fno_positions.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)

        ordersTabView = QTabWidget()
        ordersTabView.addTab(self.stocks_fno_positions, "Stocks FnO")
        ordersTabView.addTab(self.index_fno_positions, "Index")

        order_table_view = QVBoxLayout()
        order_table_view.addWidget(QLabel("Current Positions"))
        order_table_view.addWidget(ordersTabView)

        button_container = QHBoxLayout()
        self.buyButton = QPushButton("Buy")
        self.sellButton = QPushButton("Sell")

        self.buyButton.clicked.connect(self._buy_option)
        self.sellButton.clicked.connect(self._sell_option)

        self.buyButton.setEnabled(False)
        self.sellButton.setEnabled(False)

        button_container.addWidget(self.buyButton, stretch=1)
        button_container.addWidget(self.sellButton, stretch=1)

        optionsview_container.addLayout(expiry_layout)
        optionsview_container.addWidget(self.bannedWarning)
        optionsview_container.addLayout(options_table_layout)
        optionsview_container.addLayout(button_container)
        optionsview_container.addLayout(order_table_view)

        hbox_layout = QHBoxLayout()
        tab_layout = QTabWidget()
        tab_layout.addTab(self.fno_stock_list, "FnO")
        tab_layout.addTab(self.nse_stock_list, "Cash")
        hbox_layout.addWidget(tab_layout, stretch=0)
        hbox_layout.addLayout(optionsview_container, stretch=1)

        self.infoLayout = QHBoxLayout()
        self.infoLayout.addWidget(self.nameLabel)
        self.infoLayout.addStretch(1)
        self.infoLayout.addWidget(self.loginButton)

        main_layout = QVBoxLayout()
        main_layout.addLayout(self.infoLayout)
        main_layout.addLayout(hbox_layout)

        self.setLayout(main_layout)

    def _setup_ui_styling(self):
        self.bannedWarning.setStyleSheet('QLabel {font-size: 14pt; background-color:rgb(255, 165, 0);}')
        self.buyButton.setStyleSheet('QPushButton { '
                                     'background-color:rgb(0,158,0);'
                                     'color: white;'
                                     'padding: 4px;'
                                     'font: bold 13px;'
                                     'border-width: 4px;'
                                     'border-radius: 4px;'
                                     'border-color: #2752B8;'
                                     '}'
                                     'QPushButton:hover {'
                                     'background-color: rgb(0,225,0);'
                                     '}'
                                     'QPushButton:disabled {'
                                     'background-color: gray;'
                                     '}')

        self.sellButton.setStyleSheet('QPushButton { '
                                      'background-color:rgb(158,0,0);'
                                      'color: white;'
                                      'padding: 4px;'
                                      'font: bold 13px;'
                                      'border-width: 4px;'
                                      'border-radius: 4px;'
                                      'border-color: #2752B8;'
                                      '}'
                                      'QPushButton:hover {'
                                      'background-color: rgb(255,0,0);'
                                      '}'
                                      'QPushButton:disabled {'
                                      'background-color: gray;'
                                      '}'
                                      )
        self.fno_stock_list.setStyleSheet(
            'QListWidget::item {'
            'font-size: 16px;'
            'padding: 8px;'
            '}'
        )

        self.nse_stock_list.setStyleSheet(
            'QListWidget::item {'
            'font-size: 16px;'
            'padding: 8px;'
            '}'
        )

    def _setup_signals(self):
        self.on_perform_login.connect(self.shoonyaApiWrapper.onLogin)
        self.on_perform_logout.connect(self.shoonyaApiWrapper.onLogout)
        self.on_subscribe_instrument.connect(self.shoonyaApiWrapper.on_subscribe_instruments)
        self.get_positions.connect(self.shoonyaApiWrapper.on_get_positions)

        self.shoonyaApiWrapper.on_login_result.connect(self._on_login)
        self.shoonyaApiWrapper.on_price_updates.connect(self._on_price_update)
        self.shoonyaApiWrapper.on_position_result.connect(self._on_positions_results)

        self.loginButton.clicked.connect(self.on_login_clicked)
        self.fno_stock_list.itemClicked.connect(self.on_fno_stock_selected)
        self.nse_stock_list.itemClicked.connect(self.on_nse_stock_selected)
        self.expiryCombo.currentIndexChanged.connect(self.on_update_expiry_date)
        self.orderCombo.currentIndexChanged.connect(self.on_update_order_type)
        self.optionsTable.clicked.connect(self._option_selected)
        self.stocks_fno_positions.clicked.connect(self._order_selected)

    ### called when login button is clicked
    def on_login_clicked(self):
        print(f'Login button clicked on thread: ${QThread.currentThread()}')
        if not self._isLoggedIn:
            # ask for 2FA
            totp, ok_pressed = QInputDialog.getText(self, "2FA", "Enter TOTP")
            if ok_pressed and totp != '':
                # perform login
                logindata= self.cred
                logindata['totp'] = totp
                self.on_perform_login.emit(logindata)
                self.loginButton.setText("Logging in...")
        else:
            self.on_perform_logout.emit()
            self.loginButton.setText("Login")
            self.nameLabel.setText("Not Logged In")

    @Slot(bool, dict)
    def _on_login(self, success: bool, result: dict):
        logging.info(f'Login success = ${success}. result = ${result}')
        self._isLoggedIn = success
        if success:
            self.nameLabel.setText(result['uname'])
            self.loginButton.setText("Logout")
            # perform subscription to selected instrument
            self._emit_subscription()

            # fetch current open positions
            self.get_positions.emit()

        else:
            self.nameLabel.setText("Not Logged In")
            self.loginButton.setText("Login")

    def _emit_subscription(self):
        if self.currentSubscription is not None and self._isLoggedIn:
            self.on_subscribe_instrument.emit(self.currentSubscription)
            self.currentSubscription = None

    def _emit_unsubscribe(self):
        if self.currentSubscription is not None and self._isLoggedIn:
            self.on_subscribe_instrument.emit(self.currentSubscription)
            self.currentSubscription = None

    def _read_fno_master(self):
        # check if there is already a file downloaded today
        from datetime import date
        from pathlib import Path
        filename = f'NFO_{str(date.today())}.txt'
        if Path(filename).is_file():
            self.fnoData = pd.read_csv(filename, parse_dates=[5])
        else:
            r = requests.get("https://api.shoonya.com/NFO_symbols.txt.zip")
            files = ZipFile(BytesIO(r.content))
            # read the csv file with in the zip
            self.fnoData = pd.read_csv(files.open("NFO_symbols.txt"), parse_dates=[5])
            self.fnoData.to_csv(filename, index=False, header=True)

        # we are not interested in any of the NIFTY/BankNifty/FinNifty symbols as of now, so exclude them
        # also, Finvasia packages some TEST symbols in the master data, exclude them as well.
        self.fnoData = self.fnoData[~self.fnoData.Symbol.str.contains("NSETEST")][~self.fnoData.Symbol.str.contains("NIFTY")]


    def _read_nse_master(self):
        from datetime import date
        from pathlib import Path
        filename = f'NSE_{str(date.today())}.txt'
        if Path(filename).is_file():
            self.nseData = pd.read_csv(filename)
        else:
            r = requests.get("https://api.shoonya.com/NSE_symbols.txt.zip")
            files = ZipFile(BytesIO(r.content))
            self.nseData = pd.read_csv(files.open("NSE_symbols.txt"))
            self.nseData.to_csv(filename, index=False, header=True)

        # Finvasia packages some TEST symbols in the master data, exclude them as well.
        self.nseData = self.nseData[~self.nseData.Symbol.str.contains("NSETEST")]
        # Get only EQ or Index
        self.nseData = self.nseData[self.nseData['Instrument'].isin(['EQ', 'INDEX'])]


    def on_fno_download_complete(self):
        if self.fnoData is None:
            raise ValueError("Unable to read FnO master data. Can't continue")

        # read the list of stocks
        fno_stock_list = self.fnoData['Symbol'].sort_values(ascending=True).unique()

        # read the expiry dates
        fno_expiries = self.fnoData['Expiry'].sort_values(ascending=True).unique().strftime('%d-%b-%Y')

        # add the list of stocks into the stock list widget
        [self.fno_stock_list.addItem(item) for item in [QListWidgetItem(name) for name in fno_stock_list]]
        # add the expiry dates into the combo widget
        [self.expiryCombo.addItem(item) for item in fno_expiries]

    def on_nse_download_complete(self):
        if self.nseData is None:
            raise ValueError("Unable to read NSE master data. Can't continue")

        [self.nse_stock_list.addItem(item) for item in [QListWidgetItem(name) for name in self.nseData['Symbol'].sort_values(ascending=True)]]


    def on_fno_stock_selected(self, item):
        self._update_option_chain(item.text())

    def on_nse_stock_selected(self, item):
        self._update_stock_info(item.text())

    def _update_stock_info(self, item):
        logging.info(f'Update stock info for {item}')

    def _update_option_chain(self, current_stock):
        self.processUpdate = False

        self.sellButton.setEnabled(False)
        self.buyButton.setEnabled(False)

        # if there are existing subscription and we are logged in, let's unscribe from previous updates.
        self._emit_unsubscribe()

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
        self.lotSize = current_ce_chain['LotSize'].values[0]

        # create table model from the option chain and set it to the options table view
        self.optionsTable.setModel(OptionChainTableModel(data=self.currentChain))
        self.optionsTable.setItemDelegate(QHighlightDelegate(self.optionsTable.model()))

        # prepare the token list for subscribing to price updates.
        ce_subscription = [f'NFO|{name}' for name in current_ce_chain['Token']]
        pe_subscription = [f'NFO|{name}' for name in current_pe_chain['Token']]

        # save the list
        self.currentSubscription = ce_subscription + pe_subscription

        self._emit_subscription()

        self.processUpdate = True

    def on_update_expiry_date(self, new_date):
        if self.currentStock != "":
            self._update_option_chain(self.currentStock)

    def on_update_order_type(self, new_order_type):
        pass

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

        #self.sellButton.setEnabled(self.isLoggedIn and not self.bannedWarning.isVisible() and option_chain is not None)
        #self.buyButton.setEnabled(self.isLoggedIn and not self.bannedWarning.isVisible() and option_chain is not None)

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

            self.logger.info(msg=f'selected strike price {strike_price} for {option_chain} '
                  f'with Token Number: {token_number} and TradingSymbol = {trading_symbol}'
                  f' lotSize = {self.lotSize}')

            self.buyOrder = BuyOrderMarket(tradingSymbol=trading_symbol, qty=self.lotSize)
            self.sellOrder = SellOrderMarket(tradingSymbol=trading_symbol, qty=self.lotSize)

        else:
            self.logger.info(msg='User selected strike price, nothing to be done')

    @Slot(int, str, bool)
    def _on_price_update(self, token, ltp, is_banned):
        self.logger.debug(msg=f'Price update received for {token} with ltp = {ltp}. '
                              f'is the script in F&O Ban = {is_banned}, '
                              f'process this update = {self.processUpdate}')
        if not self.processUpdate or ltp == "":
            return

        self.bannedWarning.setVisible(is_banned)
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
        elif is_ce_token.size > 0:
            index_val = is_ce_token[0]
            price_col = 0
        else:
            # ignore this update
            return

        pandas_model : OptionChainTableModel = self.optionsTable.model()
        # ask the model to update the price for the said CELL.
        pandas_model.update_price(price_field, price_col, index_val, ltp)

    @Slot(bool, pd.DataFrame)
    def _on_positions_results(self, success, df):
        if not success:
            return
        self.current_positions = df
        if 'OPTSTK' in df['Type'].values:
            # these are stock options
            self.stocks_fno_positions.setModel(PositionsTableModel(df[df['Type'] == 'OPTSTK']))
        elif 'OPTIDX' in df['Type'].values:
            self.index_fno_positions.setModel(PositionsTableModel(df[df['Type'] == 'OPTIDX']))

    def _order_selected(self, item):
        pass

    def _buy_option(self):
        self.logger.debug(msg="Buy option clicked")
        #self.shoonyaAPI.placeOrder(self.buyOrder)

    def _sell_option(self):
        self.logger.debug(msg="Sell option clicked")