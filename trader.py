import dataclasses
import datetime
from abc import abstractmethod, ABC

import alpaca_trade_api.rest
from alpaca_trade_api.rest import APIError

from creds import AlpakaCreds
import alpaca_trade_api as tradeapi



@dataclasses.dataclass
class OrderData:
    quantity: int = None
    limit_price: float = None
    stop_price: float = None


class OrderType:
    market_order = "market",
    stop_loss = "stop_loss",
    limit_order = "limit",
    stop_limit_order = "stop_limit"


class BuyData:
    symbol: str
    stocks: int
    order_type: str
    buy_id: str
    buy_req_price: float
    buy_filled_price: float


class SellData:
    symbol: str
    stocks: int
    order_type: str
    sell_id: str
    sell_req_price: float
    sell_filled_price: float


class Trader(ABC):
    """Base class for all trading platform"""

    @abstractmethod
    def get_buying_power_balance(self, updated=True) -> float:
        """Get buying power balance"""

    @abstractmethod
    def buy_market_order(self, symbol: str, order_data: dict) -> BuyData:
        """Buy stock based on the param"""

    @abstractmethod
    def buy_limit_order(self, symbol: str, order_data: dict) -> BuyData:
        """Buy stock based on the param"""

    @abstractmethod
    def buy_stop_limit_order(self, symbol: str, order_data: dict) -> BuyData:
        """Buy stock based on the param"""

    @abstractmethod
    def sell_market_order(self, symbol: str, order_data: dict) -> SellData:
        """Sell stock based on the param"""

    @abstractmethod
    def sell_limit_order(self, symbol: str, order_data: dict) -> SellData:
        """Sell stock based on the param"""

    @abstractmethod
    def sell_stop_limit_order(self, symbol: str, order_data: dict) -> SellData:
        """Sell stock based on the param"""

    @abstractmethod
    def sell_status(self, sell_data: SellData) -> bool:
        """See the sell status if filled or not"""

    @abstractmethod
    def buy_status(self, buy_data: BuyData) -> bool:
        """See the buy status if filled or not"""

    @abstractmethod
    def get_order_data(self, order_id: str):
        """Order data"""


class AlpakaTrader(Trader):

    def __init__(self):
        from data_processor import TimeRangeCreator
        self.authorized_alpaka_api: alpaca_trade_api.rest.REST = None
        self.alpaka_account_info = None
        self.alpaka_cal_trading_hours = TimeRangeCreator(start_time="06:03:00", end_time="14:55:00",
                                                         interval_sec=60).get_dict()

    def set_credentials(self, alpaka_creds: AlpakaCreds):
        self.authorized_alpaka_api = self._authorize_alpaka_api(alpaka_creds)
        self.alpaka_account_info = self._get_alpaka_account()
        return self

    def _authorize_alpaka_api(self, alpaka_creds: AlpakaCreds):
        """
        creates an authorized alpaka.market api with (API_KEY, SECRET_KEY, BASE_URL)

        :return: alpaka.market authenticated api
        """

        return tradeapi.REST(
            alpaka_creds.API_KEY,
            alpaka_creds.SECRET_KEY,
            alpaka_creds.BASE_URL
        )

    def _get_alpaka_account(self):
        """
        considering that self.authorized_alpaka_api object is created and valid

        :return: account info of alpaka authenticated api
        """
        return self.authorized_alpaka_api.get_account()

    def get_allowed_buying_power_balance(self, updated=True, minus=25000):
        power = self.get_buying_power_balance(updated=updated)
        allowed = power - minus
        if allowed > 0:
            return allowed
        else:
            return 0

    def get_buying_power_balance(self, updated=True) -> float:
        if updated:
            self.alpaka_account_info = self._get_alpaka_account()
            print(self.alpaka_account_info)
        return float(self.alpaka_account_info.buying_power)

    def buy_market_order(self, symbol: str, order_data: OrderData) -> BuyData:
        """Buy at any price immediately"""
        resp = self.authorized_alpaka_api.submit_order(
            symbol=symbol,
            qty=order_data.quantity,
            side='buy',
            type="market",
            time_in_force='gtc'
        )
        print(resp)
        return resp

    def buy_limit_order(self, symbol: str, order_data: OrderData) -> BuyData:
        """buy at a limit"""
        limit_price = str(order_data.limit_price)
        resp = self.authorized_alpaka_api.submit_order(
            symbol=symbol,
            qty=order_data.quantity,
            side='buy',
            type="limit",
            time_in_force='gtc',
            limit_price=limit_price
        )
        return resp

    def buy_stop_limit_order(self, symbol: str, order_data: OrderData) -> BuyData:
        """Buy stop limit"""
        limit_price = str(order_data.limit_price)
        stop_price = str(order_data.stop_price)
        resp = self.authorized_alpaka_api.submit_order(
            symbol=symbol,
            qty=order_data.quantity,
            side='buy',
            type="stop_limit",
            time_in_force='gtc',
            limit_price=limit_price,
            stop_price=stop_price
        )
        return resp

    def sell_market_order(self, symbol: str, order_data: OrderData) -> SellData:
        resp = self.authorized_alpaka_api.submit_order(
            symbol=symbol,
            qty=order_data.quantity,
            side='sell',
            type="market",
            time_in_force='gtc'
        )
        print(resp)
        return resp

    def sell_limit_order(self, symbol: str, order_data: OrderData) -> SellData:
        """Sell Limit order"""
        limit_price = str(order_data.limit_price)
        resp = self.authorized_alpaka_api.submit_order(
            symbol=symbol,
            qty=order_data.quantity,
            side='sell',
            type="limit",
            time_in_force='gtc',
            limit_price=limit_price
        )
        return resp

    def sell_stop_limit_order(self, symbol: str, order_data: OrderData) -> SellData:
        limit_price = str(order_data.limit_price)
        stop_price = str(order_data.stop_price)
        resp = self.authorized_alpaka_api.submit_order(
            symbol=symbol,
            qty=order_data.quantity,
            side='sell',
            type="stop_limit",
            time_in_force='gtc',
            limit_price=limit_price,
            stop_price=stop_price
        )
        print(resp)
        return SellData()

    def sell_status(self, sell_data: SellData) -> bool:
        pass

    def buy_status(self, buy_data: BuyData) -> bool:
        pass

    def get_order_data(self, order_id: str):
        try:
            resp = self.authorized_alpaka_api.get_order(order_id=order_id)
            print(resp)
            return resp
        except Exception as e:
            print(e)
            print("ORDER DATA FETCHING FAILED")
            return None
        # return res

    def cancel_order(self, order_id: str):
        """Use order id to cancel the order"""
        try:
            self.authorized_alpaka_api.cancel_order(order_id=order_id)
            print("CANCEL ORDER: SUCCESS")
            return True
        except Exception as e:
            print(e)
            print("CANCEL ORDER: ORDER NOT FOUND")
            return False

    def get_order_status(self, order_id: str, order_data: alpaca_trade_api.rest.Order = None):
        if order_data is None:
            resp = self.get_order_data(order_id=order_id)
        else:
            if order_id == order_data.id:
                resp = order_data
            else:
                return None

        if resp is not None:
            return resp.status
        else:
            return None

    def is_order_filled(self, order_id: str, order_data: alpaca_trade_api.rest.Order = None):
        resp = self.get_order_status(order_id=order_id, order_data=order_data)
        if resp == "filled" or resp == "partially_filled":
            return True
        else:
            return False

    def get_filled_quantity(self, order_id: str, order_data: alpaca_trade_api.rest.Order = None):
        if order_data is None:
            resp = self.get_order_data(order_id=order_id)
        else:
            resp = order_data

        if resp is not None:
            return int(resp.filled_qty)
        else:
            return None

    def get_requested_quantity(self, order_id: str, order_data: alpaca_trade_api.rest.Order = None):
        if order_data is None:
            resp = self.get_order_data(order_id=order_id)
        else:
            resp = order_data

        if resp is not None:
            return int(resp.qty)
        else:
            return None

    def list_orders(self, status=None):
        print(self.authorized_alpaka_api.list_orders(status=status))


if __name__ == "__main__":
    # alpaka_trader = AlpakaTrader().set_credentials(AlpakaCreds())
    # alpaka_trader.cancel_order(order_id="8dc87e64-eba8-4f80-bd0d-d5a76680bac7")
    # alpaka_trader.buy_market_order("AAPL",OrderData(quantity=1))
    # print(alpaka_trader.is_order_filled(order_id="8dc87e64-eba8-4f80-bd0d-d5a76680bac7"))
    # print(alpaka_trader.get_filled_quantity(order_id="8dc87e64-eba8-4f80-bd0d-d5a76680bac7",
    # order_data=alpaka_trader.get_order_data(
    #     order_id="8dc87e64-eba8-4f80-bd0d-d5a76680bac7")))
    # print(alpaka_trader.get_order_data('2021-08-09T19:49:16.332736Z', status="FILL",
    #                                   order_id="5bfa05b2-438f-4525-8e77-da87259ce739")[0].qty)
    # alpaka_trader.list_orders(status='all')
    # print(alpaka_trader.get_buying_power_balance())
    data = [1, 2, 3, 4]
    print(data[-3:])
