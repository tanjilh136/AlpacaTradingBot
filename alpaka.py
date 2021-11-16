import alpaca_trade_api as tradeapi
import pandas as pd
from creds import AlpakaCreds


class AlpakaTrader:
    SHORTABLE = "S"
    NON_SHORTABLE = "N"
    COMPANY_FETCH_ERROR = "NOT FOUND"
    SHORTABLE_FETCH_ERROR = "NOT FOUND"

    def __init__(self, alpaka_creds: AlpakaCreds):
        self.authorized_alpaka_api = self.authorize_alpaka_api(alpaka_creds)
        self.alpaka_account_info = self.get_alpaka_account()
        self.symbol_asset = None

    def authorize_alpaka_api(self, alpaka_creds: AlpakaCreds):
        """
        creates an authorized alpaka.market api with (API_KEY, SECRET_KEY, BASE_URL)

        :return: alpaka.market authenticated api
        """

        return tradeapi.REST(
            alpaka_creds.API_KEY,
            alpaka_creds.SECRET_KEY,
            alpaka_creds.BASE_URL
        )

    def fetch_client_info_data(self):
        list_order = self.authorized_alpaka_api.list_orders(status='all')
        df = pd.DataFrame()
        ticker = []
        action = []
        share = []
        fill_price = []
        live_price = []
        status = []
        pending = []
        click_to_close = []
        click_to_close_market = []

        # Adding columns manually
        ticker.append("Ticker")
        action.append("Action")
        share.append("Share")
        fill_price.append("Fill Price")
        live_price.append("Live Price")
        status.append("Status")
        pending.append("Pending")
        click_to_close.append("Click to close")
        click_to_close_market.append("Click to close market")

        print("Fetching pending order table")
        for i in range(0, len(list_order)):
            ticker.append(list_order[i].symbol)
            action.append(list_order[i].side)
            share.append(str(list_order[i].qty))
            fill_price.append(str(list_order[i].filled_avg_price))
            live_price.append(str(self.authorized_alpaka_api.get_last_trade(list_order[i].symbol).price))
            status.append(list_order[i].status)
            pending.append("pending")
            click_to_close.append("close")
            click_to_close_market.append("close market")

        # Another column are added. Two sets of column is added, just for the help
        df["Ticker"] = ticker
        df["Action"] = action
        df["Share"] = share
        df["Fill Price"] = fill_price
        df["Live Price"] = live_price
        df["Status"] = status
        df["Pending"] = pending
        df["Click to close"] = click_to_close
        df["Click to close market"] = click_to_close_market
        print("Pending order table fetched")
        return df

    def get_alpaka_account(self):
        """
        considering that self.authorized_alpaka_api object is created and valid

        :return: account info of alpaka authenticated api
        """
        return self.authorized_alpaka_api.get_account()

    def fetch_and_set_symbol_asset(self, symbol):
        """
        Fetch asset from alpaka api and set the asset in "self.symbol_asset"

        :param symbol: name of the symbol which asset will be fetched from "self.authorized_alpaka_api" object
        :return: bool
        """
        try:
            self.symbol_asset = self.authorized_alpaka_api.get_asset(symbol)
            return True
        except Exception:
            return False

    def get_current_symbol_shortable_status(self):
        """
        shortable status of the current symbol
        self.symbol_asset contains info of the current symbol.
        :return: str - SHORTABLE/NON_SHORTABLE
        """
        if self.symbol_asset.shortable:
            return self.SHORTABLE
        else:
            return self.NON_SHORTABLE

    def get_company_name(self):
        """
        company name of the current symbol.
        self.symbol_asset contains info of the current symbol.

        :return: str - company name of the current symbol
        """
        return self.symbol_asset.name

    def get_symbol_name(self):
        """
        returns the symbol name from symbol_asset which was set earlier by calling "self.fetch_and_set_symbol_asset".

        :return: symbol
        """
        return self.symbol_asset.symbol

    def buy_order(self, order_type, symbol, quantity, **kwargs):
        """
        Buy some given quantity of given symbol based on given order_type
        Currently supports three order_types - limit, market, stop_limit.

        :param order_type: str - allowed types are limit, stop limit, market.
        :param symbol: str - ticker symbol that you want to buy.
        :param quantity: str - integer quantity as string. The quantity you want to buy.
        :param kwargs: used for other important parameter. For example limit_price when the order type is limit,
                    limit_price and stop_price when the order is stop_limit.
        :return: Order Object
        """
        if order_type == 'market':
            return self.authorized_alpaka_api.submit_order(
                symbol=symbol,
                qty=quantity,
                side='buy',
                type=order_type,
                time_in_force='gtc'
            )
        elif order_type == 'limit':
            limit_price = str(float(kwargs["limit_price"]))
            return self.authorized_alpaka_api.submit_order(
                symbol=symbol,
                qty=quantity,
                side='buy',
                type=order_type,
                time_in_force='opg',
                limit_price=limit_price
            )
        elif order_type == "stop_limit":
            limit_price = str(float(kwargs["limit_price"]))
            stop_price = str(float(kwargs["stop_price"]))
            return self.authorized_alpaka_api.submit_order(
                symbol=symbol,
                qty=quantity,
                side='buy',
                type=order_type,
                time_in_force='opg',
                limit_price=limit_price,
                stop_price=stop_price
            )

    def sell_order(self, order_type, symbol, quantity, **kwargs):
        """
        Sell some given quantity of given symbol based on given order_type
        Currently supports three order_type - limit, market, stop_limit

        :param order_type: str - allowed types are limit, stop_limit, market
        :param symbol: str - ticker symbol that you want to sell
        :param quantity: str - integer quantity as string. The quantity you want to sell
        :param kwargs: used for other important parameter. For example limiting_price when the order type is limit,
                       limit_price and stop_price when the order is stop_limit
        :return: Order_Object
        """

        if order_type == 'market':
            return self.authorized_alpaka_api.submit_order(
                symbol=symbol,
                qty=quantity,
                side='sell',
                type=order_type,
                time_in_force='gtc'
            )
        elif order_type == 'limit':
            limit_price = str(float(kwargs["limit_price"]))
            return self.authorized_alpaka_api.submit_order(
                symbol=symbol,
                qty=quantity,
                side='sell',
                type=order_type,
                time_in_force='opg',
                limit_price=limit_price
            )
        elif order_type == "stop_limit":
            limit_price = str(float(kwargs["limit_price"]))
            stop_price = str(float(kwargs["stop_price"]))
            return self.authorized_alpaka_api.submit_order(
                symbol=symbol,
                qty=quantity,
                side='sell',
                type=order_type,
                time_in_force='opg',
                limit_price=limit_price,
                stop_price=stop_price
            )

    def double_buy_sell_checker(self, symbol):
        order_list_all = self.authorized_alpaka_api.list_orders(status='all')
        for i in range(0, len(order_list_all)):
            if order_list_all[i].status == ('partially_filled' and 'filled'):
                if (order_list_all[i].symbol == symbol) and (order_list_all[i].side == 'sell'):
                    return True, order_list_all

        return False

    def double_buy(self, order_list_all, order_type, symbol, **kwargs):
        print(order_type, symbol, kwargs)
        amount_of_stock = 0
        # measuring amount of stock here
        for i in range(0, len(order_list_all)):
            if (order_list_all[i].status == 'filled') and (order_list_all[i].side == 'sell') and (
                    order_list_all[i].symbol == symbol):
                amount_of_stock = amount_of_stock + int(order_list_all[i].qty)
        print("Clicking Buy order")
        self.buy_order(order_type, symbol, (amount_of_stock * 2), **kwargs)
        print("Bought")

    def double_sell(self, order_list_all, order_type, symbol, **kwargs):
        sold_stock = 0
        bought_stock = 0
        # measuring amount of stock here
        for i in range(0, len(order_list_all)):
            if (order_list_all[i].status == 'filled') and (order_list_all[i].side == 'sell') and (
                    order_list_all[i].symbol == symbol):
                sold_stock = sold_stock + int(order_list_all[i].qty)
            if (order_list_all[i].status == 'filled') and (order_list_all[i].side == 'buy') and (
                    order_list_all[i].symbol == symbol):
                bought_stock = bought_stock + int(order_list_all[i].qty)

        self.sell_order(order_type, symbol, (bought_stock - sold_stock), **kwargs)

    def get_symbol_order_history(self, status="FILL"):
        res = self.authorized_alpaka_api.get_activities(status)
        print(res)
        return res


if __name__ == "__main__":
    # print(AlpakaTrader().buy_order(order_type="market",symbol="AAPL",quantity=100))
    # print(AlpakaTrader().buy_order(order_type="market",symbol="AAPL",quantity=100))
    pass
