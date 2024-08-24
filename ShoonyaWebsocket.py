from time import sleep
from typing import Any
import logging

from PyQt6 import QtCore

#enable dbug to see request and responses
logging.basicConfig(level=logging.DEBUG)

from PyQt6.QtCore import Qt, QThread
from api_helper import ShoonyaApiPy


class ShoonyaWebSocket(QThread):
    onPriceUpdate = QtCore.pyqtSignal(int, str, bool, name="onPriceUpdate")
    def __init__(self, api: ShoonyaApiPy, parent=None):
        super(ShoonyaWebSocket, self).__init__(parent)
        self.api = api
        self.stop = False

    def run(self):
        self.api.start_websocket(subscribe_callback=self.onSubscribe,
                                 order_update_callback=self.onOrderUpdate,
                                 socket_open_callback=self.onSocketOpen,
                                 socket_close_callback=self.onSocketClose,
                                 socket_error_callback=self.onSocketError)

    def exit(self, returnCode = 0):
        super().exit(returnCode)
        self.api.close_websocket()

    def onSubscribe(self, message):
        print(message)
        token = int(message['tk'])
        ltp = message ['lp']
        isInBan = False
        try:
            isInBan = message['s_status'] is not ""
        except:
            pass
        self.onPriceUpdate.emit(token, ltp, isInBan)



    def onOrderUpdate(self, message):
        print(f'order event ${message}')

    def onSocketOpen(self):
        print("web socket opened")

    def onSocketClose(self):
        print("web socket closed")

    def onSocketError(self):
        print("web socket error")

    def addSubscription(self, subs: Any):
        self.api.subscribe(subs)

    def removeSubcription(self, subs:Any):
        self.api.unsubscribe(subs)
