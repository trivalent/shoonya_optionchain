from NorenRestApiPy.NorenApi import NorenApi
import time
import concurrent.futures

api = None

class Order:
    def __init__(self, buy_or_sell: str = None, product_type: str = None,
                 exchange: str = None, tradingsymbol: str = None,
                 price_type: str = None, quantity: int = None,
                 price: float = None, trigger_price: float = None, discloseqty: int = 0,
                 retention: str = 'DAY', remarks: str = "tag",
                 order_id: str = None):
        self.buy_or_sell = buy_or_sell
        self.product_type = product_type
        self.exchange = exchange
        self.tradingsymbol = tradingsymbol
        self.quantity = quantity
        self.discloseqty = discloseqty
        self.price_type = price_type
        self.price = price
        self.trigger_price = trigger_price
        self.retention = retention
        self.remarks = remarks
        self.order_id = None


class BuyOrder(Order):
    """
    Use this to produce a Limit Buy Order for an Option.
    """
    def __init__(self, tradingSymbol: str, price: float, qty : int = 0):
        super().__init__(tradingsymbol=tradingSymbol, exchange='NFO',
                         product_type="M",
                         buy_or_sell='B', price_type='LMT', price=price, quantity=qty, remarks="Py_Buy_LMT")

class BuyOrderMarket(Order):
    """
    Use this to produce a Market Buy Order for an Option.
    """
    def __init__(self, tradingSymbol: str, qty: int = 0):
        super().__init__(tradingsymbol=tradingSymbol, exchange='NFO',
                         product_type="M",
                         buy_or_sell='B', price_type='MKT', price=0, quantity=qty,
                         remarks="Py_Buy_MKT")


class SellOrder(Order):
    """
    Use this to produce a Limit SELL Order for an Option.
    """
    def __init__(self, tradingSymbol: str, price: float, qty: int = 0):
        super().__init__(tradingsymbol=tradingSymbol, exchange='NFO', product_type="M",
                         buy_or_sell='S', price_type='LMT', price=price, quantity=qty,
                         remarks="Py_Sell_LMT")

class SellOrderMarket(Order):
    """
    Use this to produce a Market SELL Order for an Option.
    """
    def __init__(self, tradingSymbol: str, qty: int = 0):
        super().__init__(tradingsymbol=tradingSymbol, exchange='NFO', product_type="M",
                         buy_or_sell='S', price_type='MKT', price=0, quantity=qty,
                         remarks="Py_Sell_MKT")
# print(ret)


def get_time(time_string):
    data = time.strptime(time_string, '%d-%m-%Y %H:%M:%S')

    return time.mktime(data)


class ShoonyaApiPy(NorenApi):
    def __init__(self):
        NorenApi.__init__(self, host='https://api.shoonya.com/NorenWClientTP/',
                          websocket='wss://api.shoonya.com/NorenWSTP/')
        global api
        api = self

    def place_basket(self, orders):

        resp_err = 0
        resp_ok = 0
        result = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:

            future_to_url = {executor.submit(self.place_order, order): order for order in orders}
            for future in concurrent.futures.as_completed(future_to_url):
                url = future_to_url[future]
            try:
                result.append(future.result())
            except Exception as exc:
                print(exc)
                resp_err = resp_err + 1
            else:
                resp_ok = resp_ok + 1

        return result

    def placeOrder(self, order: Order):
        ret = NorenApi.place_order(self, buy_or_sell=order.buy_or_sell, product_type=order.product_type,
                                   exchange=order.exchange, tradingsymbol=order.tradingsymbol,
                                   quantity=order.quantity, discloseqty=order.discloseqty, price_type=order.price_type,
                                   price=order.price, trigger_price=order.trigger_price,
                                   retention=order.retention, remarks=order.remarks)
        # print(ret)

        return ret