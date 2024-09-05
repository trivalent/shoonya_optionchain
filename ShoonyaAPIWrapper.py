import json
from typing import Any
import logging

import pandas as pd
from PySide6.QtCore import Slot, QObject, Signal, QThread

from Wrapperinterface import WrapperInterface
from api_helper import ShoonyaApiPy

class ShoonyaAPIWrapper(WrapperInterface, QObject):
    logger = logging.getLogger("ShoonyaWrapper")
    # fired when we have received login result
    # bool -> if the login is successful or not. If false, the other parameters are set to None
    # dict -> the entire dictionary received as a result of login call to Shoonya API
    on_login_result = Signal(bool, dict)

    on_position_result = Signal(bool, pd.DataFrame)


    """
    int -> the token for which the signal is fired
    str -> the last traded price
    bool -> if the scrip is in ban
    """
    on_price_updates = Signal(int, str, bool)

    """
    This will be fired whenever the token specified appears in the positions as well.
    """
    on_positions_price_updates = Signal(int, str)

    def __init__(self, api: ShoonyaApiPy, parent=None):
        super().__init__(parent=parent)
        self.api = api
        self.active_subs = set()
        self.positions_subs = set()
        self._has_error = False

    @Slot(Any)
    def onLogin(self, data: Any) -> None:
        """
        This slot is invoked via the UI once it has collected all the required information for login
        The signal @{on_login_result} is emitted when the login API returns
        :param data: the required login data dictionary
        :return: None
        """
        ret = self.api.login(userid=data['user'], password=data['password'], twoFA=data['totp'],
                       vendor_code=data['vc'], imei=data['imei'], api_secret=data['apikey'])

        # if login is success, start UI update in case option chain is already selected
        self.on_login_result.emit(ret is not None, ret)

        if ret is not None:
            self.api.start_websocket(subscribe_callback=self._on_subscribe,
                                     order_update_callback=self._on_order_update,
                                     socket_open_callback=self._on_socket_open,
                                     socket_close_callback=self._on_socket_close,
                                     socket_error_callback=self._on_socket_error)

    @Slot()
    def onLogout(self):
        self.api.close_websocket()
        self.active_subs = None

    @Slot(list)
    def on_subscribe_instruments(self, data: list) -> None:
        """
        Call this slot when the UI wants to subscribe to data updates via websockets
        Various signals will be fired based upon type of data received
        :param data: The list of the instruments to subscribe to
        :return: None
        """
        self.active_subs.update(data)

        # we are assuming that multiple subscription won't actually result in multiple calls for same token
        # todo: verify the above statement
        self.api.subscribe(list(self.active_subs))

    @Slot(list)
    def on_unsubscribe_instrument(self, data: list) -> None:
        """
        Remove the update subscription for the tokens contained in data
        :param data: the token (or list of tokens) to be unsubscribed
        :return: None
        """

        self.api.unsubscribe(data)
        for x in data:
            if x in self.positions_subs:
                self.positions_subs.remove(x)
            else:
                self.active_subs.remove(x)

    def _on_subscribe(self, message):
        print(message)
        token = int(message['tk'])
        ltp = ""
        if 'lp' in message:
            ltp = message ['lp']

        is_banned = False
        try:
            is_banned = message['s_status'] != ""
        except:
            pass

        if ltp != "":
            self.on_price_updates.emit(token, ltp, is_banned)

            # fire the signal again in case this token is in positions.
            if token in self.positions_subs:
                self.on_positions_price_updates.emit(token, ltp)
        #else:
            #self.onDepthUpdate

    def _on_order_update(self, message):
        self.logger.info(f'Received order update -> {message}')

    def _on_socket_open(self):
        print("web socket opened")
        # if there was an error and current subscription is not empty, re-subscribe to get updates.
        if self._has_error and len(self.active_subs) > 0:
            self.on_subscribe_instruments(list(self.active_subs))

    def _on_socket_close(self):
        self.logger.info(f'Socket closed')

    def _on_socket_error(self, err):
        self.logger.info(f'Socket error -> {err}')
        # if we have an active subscription, set error status to True
        self._has_error = self.active_subs is not None

    @Slot()
    def on_get_positions(self):
        resp = self.api.get_positions()
        self.logger.info(f'Get positions result = {resp}')
        df = pd.DataFrame()
        if resp is not None:
            positions = pd.DataFrame.from_records(resp)
            df = pd.DataFrame()
            # ['Name', 'Expiry', 'Lots', 'Qty', 'Avg Price', 'LTP', 'P/L','Return %', 'Exchange', 'Type', 'Token']
            df['Name'] = positions['dname'].str.split().str[0]
            df['Option'] = positions['dname'].str.split(n=2).str[2]
            # assuming the mult means Lots
            df['Lots'] = positions['mult'].astype(int)
            df['Qty'] = positions['netqty'].astype(int)
            df['Avg Price'] = positions['netupldprc'].astype(float)
            df['LTP'] = positions['lp'].astype(float)
            df['P/L'] = (df['LTP'] - df['Avg Price']) * df['Qty']
            df['Return %'] = 100 * (df['P/L'] / (df['Avg Price'] * df['Qty']))
            df['Exchange'] = positions['exch']
            df['Type'] = positions['instname']
            df['Token'] = positions['token']

            # performing auto subscription of tokens for updates
            if len(self.positions_subs) > 0:
                # check if these tokens are still present in the data
                new_positions = set(positions['token'].values)
                # find if these are already subscribed
                diff = self.positions_subs - new_positions
                # update the positions
                self.positions_subs.difference_update(new_positions)
                # remove the olds
                self.on_unsubscribe_instrument(list(diff))
            else:
                self.positions_subs = set(positions['token'].values)

            new_subs = self.positions_subs - self.active_subs
            self.on_subscribe_instruments(list(new_subs))

        self.on_position_result.emit(resp is not None, df)