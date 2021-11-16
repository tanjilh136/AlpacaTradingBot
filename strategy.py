import copy
import dataclasses
import json
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Callable, Dict, List
from datetime import datetime

import requests

from stock_websocket_api import WebSocketClientClone
from custom_time import CustomTimeZone
from data_processor import TimeRangeCreator
from trader import AlpakaTrader, Trader, OrderData
from creds import AlpakaCreds, PolygonCreds

custom_t = CustomTimeZone(CustomTimeZone.CLIENT_LOCATION)  # Los_Angelos
NORMAL_MARKET = "NORMAL_MARKET"
PRE_MARKET = "PRE_MARKET"
AFTER_MARKET = "AFTER_MARKET"


def persistant_buy_sell_data(symbol: str, data, formula_buy_sell_path: str, start_date: str, start_time: str,
                             end_date="", end_time=""):
    """dump buy_sell_data of formula"""
    new_path = formula_buy_sell_path + f"/{end_date}_end_date"
    create_required_folder(new_path)
    with open(f"{new_path}/{symbol}_SD({start_date})_ST({start_time})_to_ED({end_date})_ET({end_time}).json",
              "w") as file:
        json.dump(data, file)


def create_required_folder(path_dir):
    try:
        Path(path_dir).mkdir(parents=True, exist_ok=True)
    except:
        print("Directory already exists")


BUY_WHEN_DEC = 0
BUY_WHEN_THIRD_POINT = 0


class MarketHoursCalifornia:
    def __init__(self):
        pass

    def get_pre_market_time_range_dict(self):
        return TimeRangeCreator(start_time="01:00:00", end_time="06:29:59", interval_sec=1).get_dict()

    def get_normal_hour_time_range_dict(self):
        return TimeRangeCreator(start_time="06:30:00", end_time="12:59:59", interval_sec=1).get_dict()

    def get_after_market_time_range_dict(self):
        return TimeRangeCreator(start_time="13:00:00", end_time="16:59:59", interval_sec=1).get_dict()

    def get_pre_market_time_range(self):
        # Total 5:30 hr
        return {"start": {"h": 1, "m": 0, "s": 0}, "end": {"h": 6, "m": 29, "s": 0}}

    def get_normal_hour_time_range(self):
        # Total 6:30 hr
        return {"start": {"h": 6, "m": 30, "s": 0}, "end": {"h": 12, "m": 59, "s": 0}}

    def get_after_hour_time_range(self):
        # Total 4 hr
        return {"start": {"h": 13, "m": 0, "s": 0}, "end": {"h": 16, "m": 59, "s": 0}}

    def get_market_start_end_time_range(self):
        return {"start": {"h": 1, "m": 0, "s": 0}, "end": {"h": 16, "m": 59, "s": 0}}

    def get_market_start_end_time_range_str(self):
        return {"start": "01:00:00", "end": "16:59:00"}


class BuySellEvents:
    def __init__(self, ban_mode=True):
        self.ban_mode = ban_mode
        self.current_bought_symbol = None
        self.trying_to_buy = False
        self.trying_to_sell = False
        self.sell_on_decrease = False
        self.trader_buy_requested = False
        self.trader_sell_requested = False
        self.trader_bought = False
        self.trader_sold = False
        self.sell_at_price = None
        self.trying_sell_timestamp = None
        self.selling_mode = None
        self.trader: AlpakaTrader = None
        self.processed_minute_data = None
        self.buy_commands: Dict[str, BuyCommandData] = {}
        if self.ban_mode:
            self.lost_money_on: Dict[str, int] = {}
            self.banned_symbols: Dict[str, int] = {}
        self.last_requested_qnty = 0
        self.last_buy_order_data = None
        self.last_sell_order_data = None
        self.polygon_key = PolygonCreds().secret_key
        market_hours = MarketHoursCalifornia()
        self.pre_market_hours = market_hours.get_pre_market_time_range_dict()
        self.normal_market_hours = market_hours.get_normal_hour_time_range_dict()
        self.after_market_hours = market_hours.get_after_market_time_range_dict()
        self.trader_buy_cancel_req = False
        self.place_buy_order_at_ts = 0

    def attach_trader(self, trader: Trader):
        self.trader = trader

    def attach_banned_symbols(self, banned_symbols: dict):
        self.banned_symbols = banned_symbols

    def get_buying_symbols(self):
        return self.buy_commands

    def get_current_bought_symbol(self):
        return self.current_bought_symbol

    def is_trying_buy(self):
        return self.trying_to_buy

    def is_buy_requested(self):
        return self.trader_buy_requested

    def is_bought(self):
        return self.trader_bought

    def is_sold(self):
        return self.trader_sold

    def is_sell_requested(self):
        return self.trader_sell_requested

    def is_trying_sell(self):
        return self.trying_to_sell

    def try_to_buy(self, symbol: str, at_price: float, current_timestamp: int):
        """
        Considered that self.is_trying_sell() == False
        """
        if not self.is_buy_requested():
            self.buy_commands[symbol] = BuyCommandData(symbol=symbol, buy_at=at_price, timestamp=current_timestamp)
            self.trying_to_buy = True
            print(f"Trying to buy {symbol}")

    def set_and_get_total_ema_volume(self, dataset_ema: list):
        """
        Calculate estimated moving average
        :param dataset_ema:
        :return:
        """
        total_ema_volumes = 0
        try:
            res = ((dataset_ema[5]['v'] - dataset_ema[4]['v_sma']) / 3) + dataset_ema[4]['v_sma']
            dataset_ema[5]['v_ema'] = round(res, 2)
            total_ema_volumes += dataset_ema[5]['v_ema']
            for i in range(6, len(dataset_ema)):
                res = ((dataset_ema[i]['v'] - dataset_ema[i - 1]['v_ema']) / 3) + dataset_ema[i - 1]['v_ema']
                dataset_ema[i]['v_ema'] = round(res, 2)
                total_ema_volumes += dataset_ema[i]['v_ema']
            return total_ema_volumes
        except:
            return None

    def set_sma_volume(self, dataset: list):
        """
        Generate the moving average of closing price based on minutes data
        From 6th minute c3 = 7,6,5,4,3 minutes avg
        :param dataset:
        :return:
        """
        if len(dataset) == 0:
            return None

        four_minute = 4 * 60 * 1000

        for i in range(4, len(dataset)):
            data = dataset[i]
            volumes = []
            prev_highest_timestamp = data['t'] - four_minute
            # Backtrack from current minute to previous 4 minute
            for prev_index in range(i, i - 5, -1):
                try:
                    if dataset[prev_index]['t'] >= prev_highest_timestamp:
                        # print(f"{dataset[prev_index]['c']} Found at {dataset[prev_index]['cal_t']}")
                        volumes.append(dataset[prev_index]['v'])
                    else:
                        break
                except:
                    pass

            # print("Five minutes done")
            # calculate average
            if len(volumes) > 0:
                total_vol = 0
                for vol in volumes:
                    total_vol += vol
                dataset[i]['v_sma'] = round(total_vol / len(volumes), 2)

    def get_total_volume_ema(self, symbol: str, last_minutes=30):
        if len(self.processed_minute_data[symbol]) >= last_minutes + 10:
            data = self.processed_minute_data[symbol][-last_minutes:]
            total_ema_volume = 0
            for d in data:
                total_ema_volume += d['v_ema']
            return total_ema_volume
        else:
            last_timestamp_ms = self.processed_minute_data[symbol][-1]['s']
            prev_ms = last_timestamp_ms - (72 * 60 * 60 * 1000)
            try:
                query = f"https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/minute/{prev_ms}/{last_timestamp_ms}?adjusted=true&sort=desc&limit=30&apiKey=Zay2cQZwZfUTozLiLmyprY4Sr3uK27Vp"
                resp = requests.get(query)
                res = json.loads(resp.text)
                if res['resultsCount'] > 0:
                    results = res['results']
                    results.reverse()
                    self.set_sma_volume(results)
                    return self.set_and_get_total_ema_volume(results)
                else:
                    return None

            except:
                print("Polygon rest api request failed")
                return None

    def try_to_sell(self, symbol: str, current_timestamp: int, at_price=None, selling_mode="normal"):
        """
        It must handle:
        1. If not trying to buy skip selling
        2. if trying to buy not not bought skip buying
        3. if bought try sell
        """

        if not self.buy_commands[symbol].buy_requested:
            """
            In very rare case, buy can happen even when we remove the symbol from the dictionary
            Solution: Check if the symbol exists before buying in second data
            """
            del self.buy_commands[symbol]
            return

        # Buy requested for this ticker
        self.trying_to_buy = False
        if self.is_buy_requested() and (selling_mode == 'normal'):
            print(f"Trying to sell [{selling_mode}]")
            self.sell_at_price = at_price
            self.trying_sell_timestamp = current_timestamp
            self.selling_mode = selling_mode
            self.trying_to_sell = True  # Must turn on selling mode after preparing selling creds
        elif self.is_buy_requested() and (selling_mode == 'forced'):
            print(f"Trying to sell [{selling_mode}]")
            self.selling_mode = selling_mode
            self.request_sell(timestamp=current_timestamp, symbol=self.get_current_bought_symbol(),
                              price=self.processed_minute_data[symbol][-1]['l'])
        elif self.is_buy_requested() and selling_mode == "blind":
            print(f"Trying to sell [{selling_mode}]")
            self.selling_mode = selling_mode
            self.request_sell(timestamp=current_timestamp, symbol=self.get_current_bought_symbol(),
                              price=self.processed_minute_data[symbol][-1]['l'])
        elif self.is_buy_requested():
            raise Exception("Selling type error")

    def which_market(self, cal_t: str):
        """Considered that cal_t HH:MM:SS and self.market_hours are dictionary with cal_t keys"""
        if cal_t in self.pre_market_hours:
            return PRE_MARKET
        elif cal_t in self.normal_market_hours:
            return NORMAL_MARKET
        elif cal_t in self.after_market_hours:
            return AFTER_MARKET
        return None

    def try_cancel_buy(self, symbol: str):
        """Try to cancel an order upon cancel request"""
        if not self.trader_buy_cancel_req:
            print("Trying Cancel order")
            if not self.trader.is_order_filled(order_id=self.last_buy_order_data.id):
                self.trader.cancel_order(order_id=self.last_buy_order_data.id)
                self.trader_buy_cancel_req = True
            else:
                # True means dont try cancel anymore
                self.trader_buy_cancel_req = True
                print("Order Filled. Couldnt Cancel")

    def request_buy(self, timestamp: int, symbol: str, price: float):
        self.trying_to_buy = False
        self.trader_buy_requested = True
        self.buy_commands[symbol].buy_requested = True
        self.buy_commands[symbol].buy_requested_price = price
        self.trader_sell_requested = False
        self.current_bought_symbol = symbol
        time_, date_ = custom_t.get_tz_time_date_from_timestamp(timestamp)
        if self.trader.alpaka_account_info is not None:
            print("Placing BUY order in Trader")
            st = datetime.now().timestamp()
            t_volume_ema = self.get_total_volume_ema(symbol, last_minutes=30)
            if t_volume_ema is not None:
                eq1_qty = int(t_volume_ema / 40)
            else:
                eq1_qty = 0
            print(f"EQ1: qty: {eq1_qty}")
            balance = self.trader.get_allowed_buying_power_balance(updated=True, minus=25000)
            eq2_qty = int((balance / price) * 0.95)
            print(f"EQ2: qty: {eq2_qty}")
            if eq1_qty == 0:
                self.last_requested_qnty = eq2_qty
            else:
                self.last_requested_qnty = eq1_qty if eq1_qty < eq2_qty else eq2_qty
            market_name = self.which_market(cal_t=self.processed_minute_data[symbol][-1]['cal_t'])
            order_data = None
            if market_name is None:
                raise Exception(f"MARKET NAME: {market_name}: cal_t: {self.processed_minute_data[symbol][-1]['cal_t']}")
            elif (market_name == PRE_MARKET) or (market_name == AFTER_MARKET):
                order_data = OrderData(quantity=self.last_requested_qnty, limit_price=round(price + 0.02, 2))
                self.last_buy_order_data = self.trader.buy_limit_order(symbol=symbol, order_data=order_data)
                print(
                    f"[{market_name}] Placed BUY order (limit_price = {order_data.limit_price}) in Trader: Time: {int(datetime.now().timestamp() - st)} sec")
            elif market_name == NORMAL_MARKET:
                order_data = OrderData(quantity=self.last_requested_qnty, stop_price=round(price + 0.01, 2),
                                       limit_price=round(price + 0.03, 2))
                self.last_buy_order_data = self.trader.buy_stop_limit_order(symbol=symbol, order_data=order_data)
                print(
                    f"[{market_name}] Placed BUY order (stop_price = {order_data.stop_price}, limit_price = {order_data.limit_price}) in Trader: Time: {int(datetime.now().timestamp() - st)} sec")
        print(f"Buy Request [{market_name}]".center(50, "_"))
        print(f"{symbol}".center(50, " "))
        print("".center(50, "_"))
        print(f"Time: {time_}".center(25, " "), end="")
        print(f"Date: {date_}".center(25, " "))
        if market_name == NORMAL_MARKET:
            print(f"Price: stop={order_data.stop_price},limit={order_data.limit_price}".center(25, " "), end="")
        else:
            print(f"Price: limit={order_data.limit_price}".center(25, " "), end="")
        print(f"Req_Qty: {order_data.quantity}".center(25, " "))
        print("Done".center(50, "#"))
        self.place_buy_order_at_ts = int(datetime.now().timestamp() * 1000)
        return self.place_buy_order_at_ts

    def request_sell(self, timestamp: int, symbol: str, price: float):
        self.trying_to_sell = False
        self.trying_sell_timestamp = None
        self.sell_at_price = None
        self.selling_mode = None
        self.current_bought_symbol = None
        self.trader_buy_requested = False
        self.trader_sell_requested = True

        time_, date_ = custom_t.get_tz_time_date_from_timestamp(timestamp)
        if self.trader.alpaka_account_info is not None:
            print("Placing SELL order in Trader")
            st = datetime.now().timestamp()
            order_id = self.last_buy_order_data.id
            single_bought_data = self.trader.get_order_data(order_id=order_id)
            if single_bought_data is not None:
                if self.trader.is_order_filled(order_id=order_id, order_data=single_bought_data):
                    sell_order_data = self.trader.sell_limit_order(symbol=symbol,
                                                                   order_data=OrderData(
                                                                       quantity=self.trader.get_filled_quantity(
                                                                           order_id=order_id,
                                                                           order_data=single_bought_data),
                                                                       limit_price=0.01))
                    print(sell_order_data)
                    print(f"Placed SELL order in Trader: Time: {int(datetime.now().timestamp() - st)} sec")
                    print("Sell Request".center(50, "_"))
                    print(f"{symbol}".center(50, " "))
                    print("".center(50, "_"))
                    print(f"Time: {time_}".center(25, " "), end="")
                    print(f"Date: {date_}".center(25, " "))
                    print(f"Current Price: {price}".center(25, " "), end="")
                    print(f"Possible Profit: {price - self.buy_commands[symbol].buy_requested_price}".center(25, " "))
                    print("Done".center(50, "#"))
                else:
                    print("BUY WAS NOT FILLED. Canceling if not canceled")
                    if not self.trader_buy_cancel_req:
                        self.trader.cancel_order(order_id=order_id)
            else:
                print("Order data not found to sell")
        self.trader_buy_cancel_req = False
        # If lost money 1 times
        if self.buy_commands[symbol].buy_requested_price > price:
            # Lost money detected
            self.buy_commands = {}
            if self.ban_mode:
                if symbol not in self.lost_money_on:
                    self.lost_money_on[symbol] = 1
                    # Ban it for 30 days
                    del self.lost_money_on[symbol]
                    self.banned_symbols[symbol] = timestamp + 3600 * 1000 * 24 * 30
                    return BAN_IT
            else:
                return LOST
        else:
            self.buy_commands = {}
            return PROFIT

    def attach_processed_data(self, processed_minute_data: dict):
        self.processed_minute_data = processed_minute_data

    def is_trying_sell_on_decrease(self):
        return self.sell_on_decrease

    def set_try_sell_timestamp(self, timestamp: int):
        self.trying_sell_timestamp = timestamp

    def try_sell_on_decrease(self, status: bool):
        self.sell_on_decrease = status


BAN_IT = 0
PROFIT = 1
LOST = 2


@dataclasses.dataclass
class IntersectionPoints:
    symbol: str
    pre_point_found: bool = False
    first_intersection_index: int = None
    first_intersection_found: bool = False
    second_intersection_index: int = None
    second_intersection_found: bool = False
    second_intersection_cal_t: str = None
    third_intersection_point: int = None
    highest_price_in_f_and_s_inter: float = None


@dataclasses.dataclass
class BuyCommandData:
    symbol: str
    buy_at: float
    timestamp: int
    buy_requested = False
    buy_requested_price = None


class Formula(ABC):
    @abstractmethod
    def get_banned_symbols(self):
        """Gets the banned symbol dictionary with expiration timestamp(ms) as value"""

    @abstractmethod
    def start(self):
        """Starts the algorithm"""

    @abstractmethod
    def on_second_data_received(self, second_data, symbol):
        """Second data is received with symbol name"""

    @abstractmethod
    def on_minute_data_received(self, minute_data: dict, symbol):
        """Minute data is received with symbol name"""

    @abstractmethod
    def set_sma(self, symbol, symbol_minute_datas: list):
        """Set Simple Moving Average in place"""

    @abstractmethod
    def set_ema(self, symbol, symbol_minute_datas: list):
        """Set Estimated Moving Average in place"""

    @abstractmethod
    def new_subscribed(self, symbol: str, channel: str):
        """Subscribed notification received with symbol and channel name"""

    @abstractmethod
    def new_unsubscribed(self, symbol: str, channel: str):
        """Unsubscribed notification received with symbol and channel name"""


class Formula1(Formula):
    """Sell at third intersection"""

    def __init__(self, trader: AlpakaTrader, socket_key: str, socket_uri: str, ban_mode=True, with_cancel=False,
                 cancel_price=0.03):
        self.with_cancel = with_cancel
        self.cancel_price = cancel_price
        self.market_data = WebSocketAggProvider(key=socket_key, uri=socket_uri)
        self.market_data.attach_on_minute_data_received_listener(self.on_minute_data_received)
        self.market_data.attach_on_second_data_received_listener(self.on_second_data_received)
        self.market_data.attach_new_subscribed_listener(self.new_subscribed)
        self.market_data.attach_new_unsubscribed_listener(self.new_unsubscribed)
        self.minute__agg_channel = "AM"
        self.second_agg_channel = "A"
        self.trader: AlpakaTrader = trader
        self.processed_minute_data: Dict[str, List[dict]] = {}
        self.processed_minute_intersections: Dict[str, IntersectionPoints] = {}
        self.buy_sell_events = BuySellEvents(ban_mode=ban_mode)
        self.formula_name = "formula_1_ban_" + ("yes" if ban_mode else "no")
        self.buy_sell_formula_path = f"buy_sell_data/{self.formula_name}"
        self.banned_symbols_path = f"{self.buy_sell_formula_path}/ban_list.json"
        self.buy_sell_events.attach_trader(self.trader)
        self.buy_sell_events.attach_processed_data(self.processed_minute_data)
        if ban_mode:
            self.banned_symbols = self.get_banned_symbols()
            self.buy_sell_events.attach_banned_symbols(self.banned_symbols)
        self.current_minute_timestamp = int(datetime.now().timestamp()) * 1000
        time_dict_16_59pm_to_04_02am = TimeRangeCreator(start_time="16:59:00", end_time="04:02:00",
                                                        interval_sec=1).get_dict()
        time_dict_05_59am_to_06_02am = TimeRangeCreator(start_time="05:59:00", end_time="06:02:00",
                                                        interval_sec=1).get_dict()
        time_dict_06_27am_to_06_33am = TimeRangeCreator(start_time="06:27:00", end_time="06:33:00",
                                                        interval_sec=1).get_dict()
        time_dict_12_59pm_to_13_03pm = TimeRangeCreator(start_time="12:59:00", end_time="13:03:00",
                                                        interval_sec=1).get_dict()
        self.all_excluded_times_dict = {**time_dict_16_59pm_to_04_02am, **time_dict_05_59am_to_06_02am,
                                        **time_dict_06_27am_to_06_33am, **time_dict_12_59pm_to_13_03pm}
        self.first_min_excluded_times_dict = {"16:59:00": True, "05:59:00": True, "06:27:00": True,
                                              "12:59:00": True}

        create_required_folder(self.buy_sell_formula_path)

    def get_banned_symbols(self):
        """Calls in first startup"""
        try:
            with open(self.banned_symbols_path, 'r') as file:
                data = json.load(file)
                print(data)
                return data
        except:
            return {}

    def start(self):
        self.market_data.start_fetching()

    def on_second_data_received(self, second_data, symbol):
        if symbol in self.buy_sell_events.get_buying_symbols():
            if self.buy_sell_events.is_trying_buy():
                print(f"[{symbol}] Buy Trying")
                # Buy based on characteristics
                # Intersections events are handled in minute data. here all of the prices are valid to buy
                if second_data['s'] > self.buy_sell_events.buy_commands[symbol].timestamp:
                    if second_data['h'] >= self.buy_sell_events.buy_commands[symbol].buy_at - 0.01 and \
                            self.processed_minute_data[symbol][-1]['sma'] != self.processed_minute_data[symbol][-1][
                        'ema']:
                        if (self.processed_minute_data[symbol][-1]['sma'] >
                            self.processed_minute_data[symbol][-2]['sma']) and (
                                self.processed_minute_data[symbol][-1]['ema'] >
                                self.processed_minute_data[symbol][-2]['ema']):
                            if self.is_worthy(symbol):
                                # Trend Increasing, high price is higher than buy at
                                # Request Buy NOW
                                ti, dat = custom_t.get_tz_time_date_from_timestamp(second_data['e'])
                                if ti not in self.all_excluded_times_dict:
                                    bought_at = self.buy_sell_events.buy_commands[symbol].buy_at
                                    self.buy_sell_events.request_buy(timestamp=second_data['s'], symbol=symbol,
                                                                     price=bought_at)
                                    try:
                                        # print(self.processed_minute_data[symbol])
                                        self.processed_minute_data[symbol][-1]["bought_at_timestamp"] = second_data['s']
                                        self.processed_minute_data[symbol][-1]["bought_at_price"] = bought_at
                                        persistant_buy_sell_data(symbol, data=self.processed_minute_data[symbol],
                                                                 formula_buy_sell_path=self.buy_sell_formula_path + "/realtime",
                                                                 start_date=self.processed_minute_data[symbol][0][
                                                                     'cal_d'],
                                                                 start_time=self.processed_minute_data[symbol][0][
                                                                     'cal_t'].replace(":", "_"))
                                    except:
                                        print(f"Issue on persisting {symbol} Buy Sell Update")
                                else:
                                    print("Excludend time buy skipped")
                            else:
                                print(f"{symbol} tiny data detected")
            elif self.buy_sell_events.is_trying_sell():
                if symbol == self.buy_sell_events.get_current_bought_symbol():
                    # Sell based on characteristics
                    # Intersection handled in minute data. Here all of the prices are valid to sell
                    print(f"[{symbol}] Sell Trying")
                    if second_data['s'] > self.buy_sell_events.trying_sell_timestamp:
                        # Selling at opening price
                        status = self.buy_sell_events.request_sell(timestamp=second_data['s'], symbol=symbol,
                                                                   price=second_data['o'])
                        try:
                            self.processed_minute_data[symbol][-1]["sold_at_timestamp"] = second_data['s']
                            self.processed_minute_data[symbol][-1]["sold_at_price"] = second_data['o']
                            # print(self.processed_minute_data[symbol])
                            persistant_buy_sell_data(symbol, data=self.processed_minute_data[symbol],
                                                     formula_buy_sell_path=self.buy_sell_formula_path + "/realtime",
                                                     start_date=self.processed_minute_data[symbol][0]['cal_d'],
                                                     start_time=self.processed_minute_data[symbol][0][
                                                         'cal_t'].replace(":", "_"))
                        except:
                            print(f"Issue on persisting {symbol} Buy Sell Update")
                        if status == BAN_IT:
                            # Symbol already in the banned symbols. Now dumping
                            with open(self.banned_symbols_path, 'w') as file:
                                json.dump(self.banned_symbols, file)
                            del self.processed_minute_data[symbol]
                            del self.processed_minute_intersections[symbol]
                            print(f"[Banned] {symbol} until {self.banned_symbols[symbol]}")
            if self.with_cancel:
                if self.buy_sell_events.trader_buy_requested:
                    if self.buy_sell_events.current_bought_symbol == symbol:
                        if second_data['s'] > self.buy_sell_events.place_buy_order_at_ts:
                            if second_data['h'] >= self.buy_sell_events.buy_commands[
                                symbol].buy_requested_price + self.cancel_price:
                                # cancel order if not filled
                                self.buy_sell_events.try_cancel_buy(symbol)

    def on_minute_data_received(self, minute_data: dict, symbol):
        """
        1. First intersection
        2. Second intersection
            h = highest price from 1 and 2
            buy when increasing sma and ema at high price
        """
        self.current_minute_timestamp = minute_data['s']
        if symbol not in self.processed_minute_data:
            return
        self.processed_minute_data[symbol].append(minute_data)
        time_, date_ = custom_t.get_tz_time_date_from_timestamp(minute_data['s'])
        minute_data['cal_d'] = date_
        minute_data['cal_t'] = time_
        self.set_sma(symbol, self.processed_minute_data[symbol], target_field='c')
        self.set_ema(symbol, self.processed_minute_data[symbol], target_field='c', target_sma_field='sma')
        self.set_sma(symbol, self.processed_minute_data[symbol], target_field='v')
        self.set_ema(symbol, self.processed_minute_data[symbol], target_field='v', target_sma_field='v_sma')
        current_index = len(self.processed_minute_data[symbol]) - 1

        if self.processed_minute_intersections[symbol].first_intersection_found:
            if self.processed_minute_intersections[symbol].second_intersection_found:
                if minute_data['sma'] > minute_data['ema']:
                    print(f"[{symbol}] : Third Intersection point : Time [{minute_data['cal_t']}]")
                    # Third intersection found
                    minute_data['intersection'] = "first"
                    self.processed_minute_intersections[symbol].first_intersection_index = current_index
                    self.processed_minute_intersections[symbol].second_intersection_index = None
                    self.processed_minute_intersections[symbol].second_intersection_found = False
                    self.processed_minute_intersections[symbol].second_intersection_cal_t = None
                    self.processed_minute_intersections[symbol].highest_price_in_f_and_s_inter = minute_data['h']
                    if symbol in self.buy_sell_events.get_buying_symbols():
                        self.buy_sell_events.try_to_sell(symbol=symbol, current_timestamp=minute_data['e'],
                                                         selling_mode="normal")
                elif symbol in self.buy_sell_events.get_buying_symbols():
                    if minute_data['cal_t'] in self.all_excluded_times_dict:
                        print(f"Excluded time detected. Selling immediately [{minute_data['cal_t']}]")
                        self.buy_sell_events.try_to_sell(symbol=symbol, current_timestamp=minute_data['e'],
                                                         selling_mode="forced")
            else:
                if minute_data['ema'] > minute_data['sma']:
                    print(f"[{symbol}] : Second Intersection point : Time [{minute_data['cal_t']}]")
                    minute_data['intersection'] = "second"
                    self.processed_minute_intersections[symbol].second_intersection_found = True
                    self.processed_minute_intersections[symbol].second_intersection_index = current_index
                    self.processed_minute_intersections[symbol].second_intersection_cal_t = minute_data['cal_t']
                    if self.processed_minute_intersections[
                        symbol].second_intersection_cal_t not in self.all_excluded_times_dict:
                        buy_at = round(
                            self.processed_minute_intersections[symbol].highest_price_in_f_and_s_inter + 0.01, 2)
                        if 370.5 > buy_at > 0.7:
                            if minute_data['cal_t'] in self.trader.alpaka_cal_trading_hours:
                                self.buy_sell_events.try_to_buy(symbol, buy_at, current_timestamp=minute_data['e'])
                    else:
                        print(
                            f"Excluded time detected. Skipping Buy {self.processed_minute_intersections[symbol].second_intersection_cal_t}")
                elif self.processed_minute_intersections[symbol].highest_price_in_f_and_s_inter < minute_data['h']:
                    self.processed_minute_intersections[symbol].highest_price_in_f_and_s_inter = minute_data['h']
        else:
            # Find first pre point
            if self.processed_minute_intersections[symbol].pre_point_found:
                # Find first intersection
                if minute_data['sma'] > minute_data['ema']:
                    print(f"**[{symbol}]** : Very First Intersection point : Time [{minute_data['cal_t']}]")
                    minute_data['intersection'] = "first"
                    self.processed_minute_intersections[symbol].first_intersection_index = current_index
                    self.processed_minute_intersections[symbol].first_intersection_found = True
                    self.processed_minute_intersections[symbol].highest_price_in_f_and_s_inter = minute_data['h']
            else:
                if minute_data['ema'] > minute_data['sma']:
                    print(f"*****[{symbol}] : Pre Point : Time [{minute_data['cal_t']}]")
                    minute_data['intersection'] = "pre"
                    self.processed_minute_intersections[symbol].pre_point_found = True

    def set_sma(self, symbol, symbol_minute_datas: list, target_field="c"):
        """
        Read previous data from processed minute and set sma
        Generate the moving average of closing price based on minutes data
        From 6th minute c3 = 7,6,5,4,3 minutes avg
        """
        if target_field == 'c':
            output_field = 'sma'
        elif target_field == 'v':
            output_field = 'v_sma'
        four_minute = 240000  # 60 * 1000 * 4
        current_index = len(symbol_minute_datas) - 1
        values = []
        prev_highest_timestamp = symbol_minute_datas[current_index]['s'] - four_minute
        # Backtrack from current minute to previous 4 minute
        for prev_index in range(current_index, -1, -1):
            if symbol_minute_datas[prev_index]['s'] >= prev_highest_timestamp:
                values.append(symbol_minute_datas[prev_index][target_field])
            else:
                break
        symbol_minute_datas[current_index][output_field] = round(sum(values) / len(values), 2)

    def set_ema(self, symbol, symbol_minute_datas: list, target_field='c', target_sma_field='sma'):
        """
        Read previous data from processed minute and set ema
        Calculate estimated moving average
        :return:
        """
        if target_field == 'c' and target_sma_field == 'sma':
            output_field = 'ema'
        elif target_field == 'v' and target_sma_field == 'v_sma':
            output_field = 'v_ema'

        current_index = len(symbol_minute_datas) - 1
        if current_index > 1:
            res = ((symbol_minute_datas[current_index][target_field] - symbol_minute_datas[current_index - 1][
                output_field]) / 3) + symbol_minute_datas[current_index - 1][output_field]
            symbol_minute_datas[current_index][output_field] = round(res, 2)
        elif current_index == 1:
            res = ((symbol_minute_datas[1][target_field] - symbol_minute_datas[0][target_sma_field]) / 3) + \
                  symbol_minute_datas[0][
                      target_sma_field]
            symbol_minute_datas[1][output_field] = round(res, 2)
        else:
            symbol_minute_datas[current_index][output_field] = symbol_minute_datas[current_index][target_sma_field]

    def new_subscribed(self, symbol: str, channel: str):
        """
        Prepare data storage to store processed minute data
        """
        if symbol in self.processed_minute_data:
            print("Server issue. When server closes without unsubscription")
            return
        if self.buy_sell_events.ban_mode:
            if symbol in self.banned_symbols:
                # Symbol in ban list
                if self.banned_symbols[symbol] < self.current_minute_timestamp:
                    # Symbol ban time expired
                    del self.banned_symbols[symbol]
                    with open(self.banned_symbols_path, 'w') as file:
                        json.dump(self.banned_symbols, file)
                    print(f"[UNBANNED]{symbol}")
                else:
                    print(f"[Kicked] [{symbol}] Banned Symbol Tried to get in")
                    return

        print(f"Subscription Success : {symbol} on {channel}")
        if channel == self.minute__agg_channel:
            self.processed_minute_data[symbol] = []
            self.processed_minute_intersections[symbol] = IntersectionPoints(symbol=symbol)

    def new_unsubscribed(self, symbol: str, channel: str):
        if symbol not in self.processed_minute_data:
            """
            Client missed some subscription. Skipping those
            or Client in the ban list
            """
            return
        print(f"Unsubscription Success : {symbol} on {channel}")
        if channel == self.minute__agg_channel:
            # Do what ever you want to do before deleting the stored data
            if self.buy_sell_events.is_buy_requested() and self.buy_sell_events.get_current_bought_symbol() == symbol:
                # Sell pending sell stock immediately
                current_stamp = self.processed_minute_data[symbol][-1]['e']
                self.buy_sell_events.try_to_sell(symbol=symbol, current_timestamp=current_stamp, selling_mode="blind")
                # Delete cached data
            try:
                # print(self.processed_minute_data[symbol])
                persistant_buy_sell_data(symbol, data=self.processed_minute_data[symbol],
                                         formula_buy_sell_path=self.buy_sell_formula_path + "/final",
                                         start_date=self.processed_minute_data[symbol][0]['cal_d'],
                                         start_time=self.processed_minute_data[symbol][0]['cal_t'].replace(":", "_"),
                                         end_date=self.processed_minute_data[symbol][-1]['cal_d'],
                                         end_time=self.processed_minute_data[symbol][-1]['cal_t'].replace(":", "_"),
                                         )
            except:
                print(f"Issue on persisting {symbol} buy sell data")
            if self.buy_sell_events.ban_mode:
                if symbol in self.buy_sell_events.lost_money_on:
                    print(
                        f"[Lost Money Count Reset] {symbol} lost: [{self.buy_sell_events.lost_money_on[symbol]}] times")
                    del self.buy_sell_events.lost_money_on[symbol]
            if symbol in self.buy_sell_events.buy_commands:
                # Stop buying if trying
                del self.buy_sell_events.buy_commands[symbol]
            del self.processed_minute_data[symbol]
            del self.processed_minute_intersections[symbol]

    def is_worthy(self, symbol: str):
        """Checks if the data is worthy or not"""
        i = len(self.processed_minute_data[symbol]) - 1
        t_data = self.processed_minute_data[symbol][-1]
        if t_data['v'] > 5000:
            if (round(abs(t_data['o'] - t_data['h']), 2) > 0.02) and (
                    round(abs(t_data['h'] - t_data['l']), 2) > 0.02) and (
                    round(abs(t_data['l'] - t_data['c']), 2) > 0.02) and (
                    round(abs(t_data['c'] - t_data['o']), 2) > 0.02):
                # t_data is worthy
                worthy_count = 0
                start_from = i - 4

                if start_from < 0:
                    start_from = 0
                total_data_in_check = i - start_from + 1
                for may_unworthy in self.processed_minute_data[symbol][start_from:i + 1]:
                    if (round(abs(may_unworthy['o'] - may_unworthy['h']), 2) > 0.02) and (
                            round(abs(may_unworthy['h'] - may_unworthy['l']), 2) > 0.02) and (
                            round(abs(may_unworthy['l'] - may_unworthy['c']), 2) > 0.02) and (
                            round(abs(may_unworthy['c'] - may_unworthy['o']), 2) > 0.02):
                        worthy_count += 1
                if worthy_count >= (total_data_in_check - worthy_count):
                    return True
        return False


class Formula3(Formula):
    """
    Buy after second intersection but before third intersection with higher highest price of first to second intersection.
    and sell low current low price>prev_min_low_price but if third intersection detected sell immediately
    """

    def __init__(self, trader: AlpakaTrader, socket_key: str, socket_uri: str, ban_mode=True, with_cancel=False,
                 cancel_price=0.03):
        self.with_cancel = with_cancel
        self.cancel_price = cancel_price
        self.market_data = WebSocketAggProvider(key=socket_key, uri=socket_uri)
        self.market_data.attach_on_minute_data_received_listener(self.on_minute_data_received)
        self.market_data.attach_on_second_data_received_listener(self.on_second_data_received)
        self.market_data.attach_new_subscribed_listener(self.new_subscribed)
        self.market_data.attach_new_unsubscribed_listener(self.new_unsubscribed)
        self.minute__agg_channel = "AM"
        self.second_agg_channel = "A"
        self.trader = trader
        self.processed_minute_data: Dict[str, List[dict]] = {}
        self.processed_minute_intersections: Dict[str, IntersectionPoints] = {}
        self.buy_sell_events = BuySellEvents(ban_mode=ban_mode)
        self.formula_name = "formula_3_ban_" + ("yes" if ban_mode else "no")
        self.buy_sell_formula_path = f"buy_sell_data/{self.formula_name}"
        self.banned_symbols_path = f"{self.buy_sell_formula_path}/ban_list.json"
        self.buy_sell_events.attach_trader(self.trader)
        self.buy_sell_events.attach_processed_data(self.processed_minute_data)
        if ban_mode:
            self.banned_symbols = self.get_banned_symbols()
            self.buy_sell_events.attach_banned_symbols(self.banned_symbols)
        self.current_minute_timestamp = int(datetime.now().timestamp()) * 1000
        time_dict_16_59pm_to_04_02am = TimeRangeCreator(start_time="16:59:00", end_time="04:02:00",
                                                        interval_sec=1).get_dict()
        time_dict_05_59am_to_06_02am = TimeRangeCreator(start_time="05:59:00", end_time="06:02:00",
                                                        interval_sec=1).get_dict()
        time_dict_06_27am_to_06_33am = TimeRangeCreator(start_time="06:27:00", end_time="06:33:00",
                                                        interval_sec=1).get_dict()
        time_dict_12_59pm_to_13_03pm = TimeRangeCreator(start_time="12:59:00", end_time="13:03:00",
                                                        interval_sec=1).get_dict()
        self.all_excluded_times_dict = {**time_dict_16_59pm_to_04_02am, **time_dict_05_59am_to_06_02am,
                                        **time_dict_06_27am_to_06_33am, **time_dict_12_59pm_to_13_03pm}
        self.first_min_excluded_times_dict = {"16:59:00": True, "05:59:00": True, "06:27:00": True,
                                              "12:59:00": True}

        create_required_folder(self.buy_sell_formula_path)

    def get_banned_symbols(self):
        """Calls in first startup"""
        try:
            with open(self.banned_symbols_path, 'r') as file:
                data = json.load(file)
                return data
        except:
            return {}

    def start(self):
        self.market_data.start_fetching()

    def on_second_data_received(self, second_data, symbol):
        if symbol in self.buy_sell_events.get_buying_symbols():
            if self.buy_sell_events.is_trying_buy():
                print(f"[{symbol}] Buy Trying")
                # Buy based on characteristics
                # Intersections events are handled in minute data. here all of the prices are valid to buy
                if second_data['s'] > self.buy_sell_events.buy_commands[symbol].timestamp:
                    if second_data['h'] >= self.buy_sell_events.buy_commands[symbol].buy_at - 0.01 and \
                            self.processed_minute_data[symbol][-1]['sma'] != self.processed_minute_data[symbol][-1][
                        'ema']:
                        if (self.processed_minute_data[symbol][-1]['sma'] >
                            self.processed_minute_data[symbol][-2]['sma']) and (
                                self.processed_minute_data[symbol][-1]['ema'] >
                                self.processed_minute_data[symbol][-2]['ema']):
                            if self.is_worthy(symbol):
                                # Trend Increasing, high price is higher than buy at
                                # Request Buy NOW
                                ti, dat = custom_t.get_tz_time_date_from_timestamp(second_data['e'])
                                if ti not in self.all_excluded_times_dict:
                                    bought_at = self.buy_sell_events.buy_commands[symbol].buy_at
                                    self.buy_sell_events.request_buy(timestamp=second_data['s'], symbol=symbol,
                                                                     price=bought_at)
                                    self.buy_sell_events.set_try_sell_timestamp(second_data['s'])

                                    self.buy_sell_events.try_sell_on_decrease(True)
                                    try:
                                        # print(self.processed_minute_data[symbol])
                                        self.processed_minute_data[symbol][-1]["bought_at_timestamp"] = second_data['s']
                                        self.processed_minute_data[symbol][-1]["bought_at_price"] = bought_at
                                        persistant_buy_sell_data(symbol, data=self.processed_minute_data[symbol],
                                                                 formula_buy_sell_path=self.buy_sell_formula_path + "/realtime",
                                                                 start_date=self.processed_minute_data[symbol][0][
                                                                     'cal_d'],
                                                                 start_time=self.processed_minute_data[symbol][0][
                                                                     'cal_t'].replace(":", "_"))
                                    except:
                                        print(f"Issue on persisting {symbol} Buy Sell Update")
                            else:
                                print(f"{symbol} tiny data detected")
            elif self.buy_sell_events.is_trying_sell_on_decrease():
                if symbol == self.buy_sell_events.get_current_bought_symbol():
                    if second_data['s'] > self.buy_sell_events.trying_sell_timestamp:
                        print(f"[{symbol}] Sell Trying to find decrease")
                        if self.processed_minute_data[symbol][-1]['l'] > second_data['l']:  # Decrease detected:
                            # Selling at opening price
                            status = self.buy_sell_events.request_sell(timestamp=second_data['s'], symbol=symbol,
                                                                       price=self.processed_minute_data[symbol][-1][
                                                                                 'l'] - 0.01)
                            self.buy_sell_events.try_sell_on_decrease(False)
                            try:
                                self.processed_minute_data[symbol][-1]["sold_at_timestamp"] = second_data['s']
                                self.processed_minute_data[symbol][-1]["sold_at_price"] = second_data['l'] - 0.01
                                # print(self.processed_minute_data[symbol])
                                persistant_buy_sell_data(symbol, data=self.processed_minute_data[symbol],
                                                         formula_buy_sell_path=self.buy_sell_formula_path + "/realtime",
                                                         start_date=self.processed_minute_data[symbol][0]['cal_d'],
                                                         start_time=self.processed_minute_data[symbol][0][
                                                             'cal_t'].replace(":", "_"))
                            except:
                                print(f"Issue on persisting {symbol} Buy Sell Update")
                            if status == BAN_IT:
                                # Symbol already in the banned symbols. Now dumping
                                with open(self.banned_symbols_path, 'w') as file:
                                    json.dump(self.banned_symbols, file)
                                del self.processed_minute_data[symbol]
                                del self.processed_minute_intersections[symbol]
                                print(f"[Banned] {symbol}")
            elif self.buy_sell_events.is_trying_sell():
                if symbol == self.buy_sell_events.get_current_bought_symbol():
                    # Sell based on characteristics
                    # Intersection handled in minute data. Here all of the prices are valid to sell
                    print(f"[{symbol}] Sell Trying on Third Intersection")
                    if second_data['s'] > self.buy_sell_events.trying_sell_timestamp:
                        # Selling at opening price
                        status = self.buy_sell_events.request_sell(timestamp=second_data['s'], symbol=symbol,
                                                                   price=second_data['o'])
                        try:
                            self.processed_minute_data[symbol][-1]["sold_at_timestamp"] = second_data['s']
                            self.processed_minute_data[symbol][-1]["sold_at_price"] = second_data['o']
                            # print(self.processed_minute_data[symbol])
                            persistant_buy_sell_data(symbol, data=self.processed_minute_data[symbol],
                                                     formula_buy_sell_path=self.buy_sell_formula_path + "/realtime",
                                                     start_date=self.processed_minute_data[symbol][0]['cal_d'],
                                                     start_time=self.processed_minute_data[symbol][0][
                                                         'cal_t'].replace(":", "_"))
                        except:
                            print(f"Issue on persisting {symbol} Buy Sell Update")
                        if status == BAN_IT:
                            # Symbol already in the banned symbols. Now dumping
                            with open(self.banned_symbols_path, 'w') as file:
                                json.dump(self.banned_symbols, file)
                            del self.processed_minute_data[symbol]
                            del self.processed_minute_intersections[symbol]
                            print(f"[Banned] {symbol}")
            if self.with_cancel:
                if self.buy_sell_events.trader_buy_requested:
                    if self.buy_sell_events.current_bought_symbol == symbol:
                        if second_data['s'] > self.buy_sell_events.place_buy_order_at_ts:
                            if second_data['h'] >= self.buy_sell_events.buy_commands[
                                symbol].buy_requested_price + self.cancel_price:
                                # cancel order if not filled
                                self.buy_sell_events.try_cancel_buy(symbol)

    def on_minute_data_received(self, minute_data: dict, symbol):
        """
        1. First intersection
        2. Second intersection
            h = highest price from 1 and 2
            buy when increasing sma and ema at high price
        """
        self.current_minute_timestamp = minute_data['s']
        if symbol not in self.processed_minute_data:
            return
        self.processed_minute_data[symbol].append(minute_data)
        time_, date_ = custom_t.get_tz_time_date_from_timestamp(minute_data['s'])
        minute_data['cal_d'] = date_
        minute_data['cal_t'] = time_
        self.set_sma(symbol, self.processed_minute_data[symbol], target_field='c')
        self.set_ema(symbol, self.processed_minute_data[symbol], target_field='c', target_sma_field='sma')
        self.set_sma(symbol, self.processed_minute_data[symbol], target_field='v')
        self.set_ema(symbol, self.processed_minute_data[symbol], target_field='v', target_sma_field='v_sma')
        current_index = len(self.processed_minute_data[symbol]) - 1

        if self.processed_minute_intersections[symbol].first_intersection_found:
            if self.processed_minute_intersections[symbol].second_intersection_found:
                if minute_data['sma'] > minute_data['ema']:
                    print(f"[{symbol}] : Third Intersection point : Time [{minute_data['cal_t']}]")
                    # Third intersection found
                    minute_data['intersection'] = "first"
                    self.processed_minute_intersections[symbol].first_intersection_index = current_index
                    self.processed_minute_intersections[symbol].second_intersection_index = None
                    self.processed_minute_intersections[symbol].second_intersection_found = False
                    self.processed_minute_intersections[symbol].second_intersection_cal_t = None
                    self.processed_minute_intersections[symbol].highest_price_in_f_and_s_inter = minute_data['h']
                    if symbol in self.buy_sell_events.get_buying_symbols():
                        self.buy_sell_events.try_sell_on_decrease(False)
                        self.buy_sell_events.try_to_sell(symbol=symbol, current_timestamp=minute_data['e'],
                                                         selling_mode="normal")
                elif symbol in self.buy_sell_events.get_buying_symbols():
                    if minute_data['cal_t'] in self.all_excluded_times_dict:
                        print(f"Excluded time detected. Selling immediately [{minute_data['cal_t']}]")
                        self.buy_sell_events.try_sell_on_decrease(False)
                        self.buy_sell_events.try_to_sell(symbol=symbol, current_timestamp=minute_data['e'],
                                                         selling_mode="forced")
            else:
                if minute_data['ema'] > minute_data['sma']:
                    print(f"[{symbol}] : Second Intersection point : Time [{minute_data['cal_t']}]")
                    minute_data['intersection'] = "second"
                    self.processed_minute_intersections[symbol].second_intersection_found = True
                    self.processed_minute_intersections[symbol].second_intersection_index = current_index
                    self.processed_minute_intersections[symbol].second_intersection_cal_t = minute_data['cal_t']
                    if self.processed_minute_intersections[
                        symbol].second_intersection_cal_t not in self.all_excluded_times_dict:
                        buy_at = round(
                            self.processed_minute_intersections[symbol].highest_price_in_f_and_s_inter + 0.01, 2)
                        if 370.5 > buy_at > 0.7:
                            if minute_data['cal_t'] in self.trader.alpaka_cal_trading_hours:
                                self.buy_sell_events.try_to_buy(symbol, buy_at, current_timestamp=minute_data['e'])
                    else:
                        print(
                            f"Excluded time detected. Skipping Buy {self.processed_minute_intersections[symbol].second_intersection_cal_t}")
                elif self.processed_minute_intersections[symbol].highest_price_in_f_and_s_inter < minute_data['h']:
                    self.processed_minute_intersections[symbol].highest_price_in_f_and_s_inter = minute_data['h']
        else:
            # Find first pre point
            if self.processed_minute_intersections[symbol].pre_point_found:
                # Find first intersection
                if minute_data['sma'] > minute_data['ema']:
                    print(f"**[{symbol}]** : Very First Intersection point : Time [{minute_data['cal_t']}]")
                    minute_data['intersection'] = "first"
                    self.processed_minute_intersections[symbol].first_intersection_index = current_index
                    self.processed_minute_intersections[symbol].first_intersection_found = True
                    self.processed_minute_intersections[symbol].highest_price_in_f_and_s_inter = minute_data['h']
            else:
                if minute_data['ema'] > minute_data['sma']:
                    print(f"*****[{symbol}] : Pre Point : Time [{minute_data['cal_t']}]")
                    minute_data['intersection'] = "pre"
                    self.processed_minute_intersections[symbol].pre_point_found = True

    def set_sma(self, symbol, symbol_minute_datas: list, target_field="c"):
        """
        Read previous data from processed minute and set sma
        Generate the moving average of closing price based on minutes data
        From 6th minute c3 = 7,6,5,4,3 minutes avg
        """
        if target_field == 'c':
            output_field = 'sma'
        elif target_field == 'v':
            output_field = 'v_sma'
        four_minute = 240000  # 60 * 1000 * 4
        current_index = len(symbol_minute_datas) - 1
        values = []
        prev_highest_timestamp = symbol_minute_datas[current_index]['s'] - four_minute
        # Backtrack from current minute to previous 4 minute
        for prev_index in range(current_index, -1, -1):
            if symbol_minute_datas[prev_index]['s'] >= prev_highest_timestamp:
                values.append(symbol_minute_datas[prev_index][target_field])
            else:
                break
        symbol_minute_datas[current_index][output_field] = round(sum(values) / len(values), 2)

    def set_ema(self, symbol, symbol_minute_datas: list, target_field='c', target_sma_field='sma'):
        """
        Read previous data from processed minute and set ema
        Calculate estimated moving average
        :return:
        """
        if target_field == 'c' and target_sma_field == 'sma':
            output_field = 'ema'
        elif target_field == 'v' and target_sma_field == 'v_sma':
            output_field = 'v_ema'

        current_index = len(symbol_minute_datas) - 1
        if current_index > 1:
            res = ((symbol_minute_datas[current_index][target_field] - symbol_minute_datas[current_index - 1][
                output_field]) / 3) + symbol_minute_datas[current_index - 1][output_field]
            symbol_minute_datas[current_index][output_field] = round(res, 2)
        elif current_index == 1:
            res = ((symbol_minute_datas[1][target_field] - symbol_minute_datas[0][target_sma_field]) / 3) + \
                  symbol_minute_datas[0][
                      target_sma_field]
            symbol_minute_datas[1][output_field] = round(res, 2)
        else:
            symbol_minute_datas[current_index][output_field] = symbol_minute_datas[current_index][target_sma_field]

    def new_subscribed(self, symbol: str, channel: str):
        """
        Prepare data storage to store processed minute data
        """
        if symbol in self.processed_minute_data:
            print("Server issue. When server closes without unsubscription")
            return
        if self.buy_sell_events.ban_mode:
            if symbol in self.banned_symbols:
                # Symbol in ban list
                if self.banned_symbols[symbol] < self.current_minute_timestamp:
                    # Symbol ban time expired
                    del self.banned_symbols[symbol]
                    with open(self.banned_symbols_path, 'w') as file:
                        json.dump(self.banned_symbols, file)
                    print(f"[UNBANNED]{symbol}")
                else:
                    print(f"[Kicked] [{symbol}] Banned Symbol Tried to get in")
                    return

        print(f"Subscription Success : {symbol} on {channel}")
        if channel == self.minute__agg_channel:
            self.processed_minute_data[symbol] = []
            self.processed_minute_intersections[symbol] = IntersectionPoints(symbol=symbol)

    def new_unsubscribed(self, symbol: str, channel: str):
        if symbol not in self.processed_minute_data:
            """
            Client missed some subscription. Skipping those
            or Client in the ban list
            """
            return
        print(f"Unsubscription Success : {symbol} on {channel}")
        if channel == self.minute__agg_channel:
            # Do what ever you want to do before deleting the stored data
            if self.buy_sell_events.is_buy_requested() and self.buy_sell_events.get_current_bought_symbol() == symbol:
                # Sell pending sell stock immediately
                current_stamp = self.processed_minute_data[symbol][-1]['e']
                self.buy_sell_events.try_to_sell(symbol=symbol, current_timestamp=current_stamp, selling_mode="blind")
                # Delete cached data
            try:
                # print(self.processed_minute_data[symbol])
                persistant_buy_sell_data(symbol, data=self.processed_minute_data[symbol],
                                         formula_buy_sell_path=self.buy_sell_formula_path + "/final",
                                         start_date=self.processed_minute_data[symbol][0]['cal_d'],
                                         start_time=self.processed_minute_data[symbol][0]['cal_t'].replace(":", "_"),
                                         end_date=self.processed_minute_data[symbol][-1]['cal_d'],
                                         end_time=self.processed_minute_data[symbol][-1]['cal_t'].replace(":", "_"),
                                         )
            except:
                print(f"Issue on persisting {symbol} buy sell data")
            if self.buy_sell_events.ban_mode:
                if symbol in self.buy_sell_events.lost_money_on:
                    print(
                        f"[Lost Money Count Reset] {symbol} lost: [{self.buy_sell_events.lost_money_on[symbol]}] times")
                    del self.buy_sell_events.lost_money_on[symbol]
            if symbol in self.buy_sell_events.buy_commands:
                # Stop buying if trying
                del self.buy_sell_events.buy_commands[symbol]
            del self.processed_minute_data[symbol]
            del self.processed_minute_intersections[symbol]

    def is_worthy(self, symbol: str):
        """Checks if the data is worthy or not"""
        i = len(self.processed_minute_data[symbol]) - 1
        t_data = self.processed_minute_data[symbol][-1]
        if t_data['v'] > 5000:
            if (round(abs(t_data['o'] - t_data['h']), 2) > 0.02) and (
                    round(abs(t_data['h'] - t_data['l']), 2) > 0.02) and (
                    round(abs(t_data['l'] - t_data['c']), 2) > 0.02) and (
                    round(abs(t_data['c'] - t_data['o']), 2) > 0.02):
                # t_data is worthy
                worthy_count = 0
                start_from = i - 4

                if start_from < 0:
                    start_from = 0
                total_data_in_check = i - start_from + 1
                for may_unworthy in self.processed_minute_data[symbol][start_from:i + 1]:
                    if (round(abs(may_unworthy['o'] - may_unworthy['h']), 2) > 0.02) and (
                            round(abs(may_unworthy['h'] - may_unworthy['l']), 2) > 0.02) and (
                            round(abs(may_unworthy['l'] - may_unworthy['c']), 2) > 0.02) and (
                            round(abs(may_unworthy['c'] - may_unworthy['o']), 2) > 0.02):
                        worthy_count += 1
                if worthy_count >= (total_data_in_check - worthy_count):
                    return True
        return False


class Formula4(Formula):
    """
    Buy after second intersection but before third intersection.
    and sell after third intersection when low price is decreasing
    """

    def __init__(self, trader: AlpakaTrader, socket_key: str, socket_uri: str, ban_mode=True, with_cancel=False,
                 cancel_price=0.03):
        self.with_cancel = with_cancel
        self.cancel_price = cancel_price
        self.market_data = WebSocketAggProvider(key=socket_key, uri=socket_uri)
        self.market_data.attach_on_minute_data_received_listener(self.on_minute_data_received)
        self.market_data.attach_on_second_data_received_listener(self.on_second_data_received)
        self.market_data.attach_new_subscribed_listener(self.new_subscribed)
        self.market_data.attach_new_unsubscribed_listener(self.new_unsubscribed)
        self.minute__agg_channel = "AM"
        self.second_agg_channel = "A"
        self.trader = trader
        self.processed_minute_data: Dict[str, List[dict]] = {}
        self.processed_minute_intersections: Dict[str, IntersectionPoints] = {}
        self.buy_sell_events = BuySellEvents(ban_mode=ban_mode)
        self.formula_name = "formula_4_ban_" + ("yes" if ban_mode else "no")
        self.buy_sell_formula_path = f"buy_sell_data/{self.formula_name}"
        self.banned_symbols_path = f"{self.buy_sell_formula_path}/ban_list.json"
        self.buy_sell_events.attach_trader(self.trader)
        self.buy_sell_events.attach_processed_data(self.processed_minute_data)
        if ban_mode:
            self.banned_symbols = self.get_banned_symbols()
            self.buy_sell_events.attach_banned_symbols(self.banned_symbols)
        self.current_minute_timestamp = int(datetime.now().timestamp()) * 1000
        time_dict_16_59pm_to_04_02am = TimeRangeCreator(start_time="16:59:00", end_time="04:02:00",
                                                        interval_sec=1).get_dict()
        time_dict_05_59am_to_06_02am = TimeRangeCreator(start_time="05:59:00", end_time="06:02:00",
                                                        interval_sec=1).get_dict()
        time_dict_06_27am_to_06_33am = TimeRangeCreator(start_time="06:27:00", end_time="06:33:00",
                                                        interval_sec=1).get_dict()
        time_dict_12_59pm_to_13_03pm = TimeRangeCreator(start_time="12:59:00", end_time="13:03:00",
                                                        interval_sec=1).get_dict()
        self.all_excluded_times_dict = {**time_dict_16_59pm_to_04_02am, **time_dict_05_59am_to_06_02am,
                                        **time_dict_06_27am_to_06_33am, **time_dict_12_59pm_to_13_03pm}
        self.first_min_excluded_times_dict = {"16:59:00": True, "05:59:00": True, "06:27:00": True,
                                              "12:59:00": True}

        create_required_folder(self.buy_sell_formula_path)

    def get_banned_symbols(self):
        """Calls in first startup"""
        try:
            with open(self.banned_symbols_path, 'r') as file:
                data = json.load(file)
                print(data)
                print(type(data))
                return data
        except:
            return {}

    def start(self):
        self.market_data.start_fetching()

    def on_second_data_received(self, second_data, symbol):
        if symbol in self.buy_sell_events.get_buying_symbols():
            if self.buy_sell_events.is_trying_buy():
                print(f"[{symbol}] Buy Trying")
                # Buy based on characteristics
                # Intersections events are handled in minute data. here all of the prices are valid to buy
                if second_data['s'] > self.buy_sell_events.buy_commands[symbol].timestamp:
                    if second_data['h'] >= self.buy_sell_events.buy_commands[symbol].buy_at - 0.01 and \
                            self.processed_minute_data[symbol][-1]['sma'] != self.processed_minute_data[symbol][-1][
                        'ema']:
                        if (self.processed_minute_data[symbol][-1]['sma'] >
                            self.processed_minute_data[symbol][-2]['sma']) and (
                                self.processed_minute_data[symbol][-1]['ema'] >
                                self.processed_minute_data[symbol][-2]['ema']):
                            if self.is_worthy(symbol):
                                # Trend Increasing, high price is higher than buy at
                                # Request Buy NOW
                                ti, dat = custom_t.get_tz_time_date_from_timestamp(second_data['e'])
                                if ti not in self.all_excluded_times_dict:
                                    bought_at = self.buy_sell_events.buy_commands[symbol].buy_at
                                    self.buy_sell_events.request_buy(timestamp=second_data['s'], symbol=symbol,
                                                                     price=bought_at)
                                    try:
                                        # print(self.processed_minute_data[symbol])
                                        self.processed_minute_data[symbol][-1]["bought_at_timestamp"] = second_data['s']
                                        self.processed_minute_data[symbol][-1]["bought_at_price"] = bought_at
                                        persistant_buy_sell_data(symbol, data=self.processed_minute_data[symbol],
                                                                 formula_buy_sell_path=self.buy_sell_formula_path + "/realtime",
                                                                 start_date=self.processed_minute_data[symbol][0][
                                                                     'cal_d'],
                                                                 start_time=self.processed_minute_data[symbol][0][
                                                                     'cal_t'].replace(":", "_"))
                                    except:
                                        print(f"Issue on persisting {symbol} Buy Sell Update")
                            else:
                                print(f"{symbol} tiny data detected")
            elif self.buy_sell_events.is_trying_sell():
                if symbol == self.buy_sell_events.get_current_bought_symbol():
                    # Sell based on characteristics
                    # Intersection handled in minute data. Here all of the prices are valid to sell
                    print(f"[{symbol}] Sell Trying when decreasing")
                    if second_data['s'] > self.buy_sell_events.trying_sell_timestamp:
                        # Selling at opening price
                        if self.processed_minute_data[symbol][-1]['l'] > second_data['l']:
                            status = self.buy_sell_events.request_sell(timestamp=second_data['s'], symbol=symbol,
                                                                       price=self.processed_minute_data[symbol][-1][
                                                                                 'l'] - 0.01)
                            try:
                                self.processed_minute_data[symbol][-1]["sold_at_timestamp"] = second_data['s']
                                self.processed_minute_data[symbol][-1]["sold_at_price"] = second_data['o']
                                # print(self.processed_minute_data[symbol])
                                persistant_buy_sell_data(symbol, data=self.processed_minute_data[symbol],
                                                         formula_buy_sell_path=self.buy_sell_formula_path + "/realtime",
                                                         start_date=self.processed_minute_data[symbol][0]['cal_d'],
                                                         start_time=self.processed_minute_data[symbol][0][
                                                             'cal_t'].replace(":", "_"))
                            except:
                                print(f"Issue on persisting {symbol} Buy Sell Update")
                            if status == BAN_IT:
                                # Symbol already in the banned symbols. Now dumping
                                with open(self.banned_symbols_path, 'w') as file:
                                    json.dump(self.banned_symbols, file)
                                del self.processed_minute_data[symbol]
                                del self.processed_minute_intersections[symbol]
                                print(f"[Banned] {symbol}")
            if self.with_cancel:
                if self.buy_sell_events.trader_buy_requested:
                    if self.buy_sell_events.current_bought_symbol == symbol:
                        if second_data['s'] > self.buy_sell_events.place_buy_order_at_ts:
                            if second_data['h'] >= self.buy_sell_events.buy_commands[
                                symbol].buy_requested_price + self.cancel_price:
                                # cancel order if not filled
                                self.buy_sell_events.try_cancel_buy(symbol)

    def on_minute_data_received(self, minute_data: dict, symbol):
        """
        1. First intersection
        2. Second intersection
            h = highest price from 1 and 2
            buy when increasing sma and ema at high price
        """
        self.current_minute_timestamp = minute_data['s']
        if symbol not in self.processed_minute_data:
            return
        self.processed_minute_data[symbol].append(minute_data)
        time_, date_ = custom_t.get_tz_time_date_from_timestamp(minute_data['s'])
        minute_data['cal_d'] = date_
        minute_data['cal_t'] = time_
        self.set_sma(symbol, self.processed_minute_data[symbol], target_field='c')
        self.set_ema(symbol, self.processed_minute_data[symbol], target_field='c', target_sma_field='sma')
        self.set_sma(symbol, self.processed_minute_data[symbol], target_field='v')
        self.set_ema(symbol, self.processed_minute_data[symbol], target_field='v', target_sma_field='v_sma')
        current_index = len(self.processed_minute_data[symbol]) - 1

        if self.processed_minute_intersections[symbol].first_intersection_found:
            if self.processed_minute_intersections[symbol].second_intersection_found:
                if minute_data['sma'] > minute_data['ema']:
                    print(f"[{symbol}] : Third Intersection point : Time [{minute_data['cal_t']}]")
                    # Third intersection found
                    minute_data['intersection'] = "first"
                    self.processed_minute_intersections[symbol].first_intersection_index = current_index
                    self.processed_minute_intersections[symbol].second_intersection_index = None
                    self.processed_minute_intersections[symbol].second_intersection_found = False
                    self.processed_minute_intersections[symbol].second_intersection_cal_t = None
                    self.processed_minute_intersections[symbol].highest_price_in_f_and_s_inter = minute_data['h']
                    if symbol in self.buy_sell_events.get_buying_symbols():
                        self.buy_sell_events.try_to_sell(symbol=symbol, current_timestamp=minute_data['e'],
                                                         selling_mode="normal")
                elif symbol in self.buy_sell_events.get_buying_symbols():
                    if minute_data['cal_t'] in self.all_excluded_times_dict:
                        print(f"Excluded time detected. Selling immediately [{minute_data['cal_t']}]")
                        self.buy_sell_events.try_to_sell(symbol=symbol, current_timestamp=minute_data['e'],
                                                         selling_mode="forced")
            else:
                if minute_data['ema'] > minute_data['sma']:
                    print(f"[{symbol}] : Second Intersection point : Time [{minute_data['cal_t']}]")
                    minute_data['intersection'] = "second"
                    self.processed_minute_intersections[symbol].second_intersection_found = True
                    self.processed_minute_intersections[symbol].second_intersection_index = current_index
                    self.processed_minute_intersections[symbol].second_intersection_cal_t = minute_data['cal_t']
                    if self.processed_minute_intersections[
                        symbol].second_intersection_cal_t not in self.all_excluded_times_dict:
                        buy_at = round(
                            self.processed_minute_intersections[symbol].highest_price_in_f_and_s_inter + 0.01, 2)
                        if 370.5 > buy_at > 0.7:
                            if minute_data['cal_t'] in self.trader.alpaka_cal_trading_hours:
                                self.buy_sell_events.try_to_buy(symbol, buy_at, current_timestamp=minute_data['e'])
                    else:
                        print(
                            f"Excluded time detected. Skipping Buy {self.processed_minute_intersections[symbol].second_intersection_cal_t}")
                elif self.processed_minute_intersections[symbol].highest_price_in_f_and_s_inter < minute_data['h']:
                    self.processed_minute_intersections[symbol].highest_price_in_f_and_s_inter = minute_data['h']
        else:
            # Find first pre point
            if self.processed_minute_intersections[symbol].pre_point_found:
                # Find first intersection
                if minute_data['sma'] > minute_data['ema']:
                    print(f"**[{symbol}]** : Very First Intersection point : Time [{minute_data['cal_t']}]")
                    minute_data['intersection'] = "first"
                    self.processed_minute_intersections[symbol].first_intersection_index = current_index
                    self.processed_minute_intersections[symbol].first_intersection_found = True
                    self.processed_minute_intersections[symbol].highest_price_in_f_and_s_inter = minute_data['h']
            else:
                if minute_data['ema'] > minute_data['sma']:
                    print(f"*****[{symbol}] : Pre Point : Time [{minute_data['cal_t']}]")
                    minute_data['intersection'] = "pre"
                    self.processed_minute_intersections[symbol].pre_point_found = True

    def set_sma(self, symbol, symbol_minute_datas: list, target_field="c"):
        """
        Read previous data from processed minute and set sma
        Generate the moving average of closing price based on minutes data
        From 6th minute c3 = 7,6,5,4,3 minutes avg
        """
        if target_field == 'c':
            output_field = 'sma'
        elif target_field == 'v':
            output_field = 'v_sma'
        four_minute = 240000  # 60 * 1000 * 4
        current_index = len(symbol_minute_datas) - 1
        values = []
        prev_highest_timestamp = symbol_minute_datas[current_index]['s'] - four_minute
        # Backtrack from current minute to previous 4 minute
        for prev_index in range(current_index, -1, -1):
            if symbol_minute_datas[prev_index]['s'] >= prev_highest_timestamp:
                values.append(symbol_minute_datas[prev_index][target_field])
            else:
                break
        symbol_minute_datas[current_index][output_field] = round(sum(values) / len(values), 2)

    def set_ema(self, symbol, symbol_minute_datas: list, target_field='c', target_sma_field='sma'):
        """
        Read previous data from processed minute and set ema
        Calculate estimated moving average
        :return:
        """
        if target_field == 'c' and target_sma_field == 'sma':
            output_field = 'ema'
        elif target_field == 'v' and target_sma_field == 'v_sma':
            output_field = 'v_ema'

        current_index = len(symbol_minute_datas) - 1
        if current_index > 1:
            res = ((symbol_minute_datas[current_index][target_field] - symbol_minute_datas[current_index - 1][
                output_field]) / 3) + symbol_minute_datas[current_index - 1][output_field]
            symbol_minute_datas[current_index][output_field] = round(res, 2)
        elif current_index == 1:
            res = ((symbol_minute_datas[1][target_field] - symbol_minute_datas[0][target_sma_field]) / 3) + \
                  symbol_minute_datas[0][
                      target_sma_field]
            symbol_minute_datas[1][output_field] = round(res, 2)
        else:
            symbol_minute_datas[current_index][output_field] = symbol_minute_datas[current_index][target_sma_field]

    def new_subscribed(self, symbol: str, channel: str):
        """
        Prepare data storage to store processed minute data
        """
        if symbol in self.processed_minute_data:
            print("Server issue. When server closes without unsubscription")
            return
        if self.buy_sell_events.ban_mode:
            if symbol in self.banned_symbols:
                # Symbol in ban list
                if self.banned_symbols[symbol] < self.current_minute_timestamp:
                    # Symbol ban time expired
                    del self.banned_symbols[symbol]
                    with open(self.banned_symbols_path, 'w') as file:
                        json.dump(self.banned_symbols, file)
                    print(f"[UNBANNED]{symbol}")
                else:
                    print(f"[Kicked] [{symbol}] Banned Symbol Tried to get in")
                    return

        print(f"Subscription Success : {symbol} on {channel}")
        if channel == self.minute__agg_channel:
            self.processed_minute_data[symbol] = []
            self.processed_minute_intersections[symbol] = IntersectionPoints(symbol=symbol)

    def new_unsubscribed(self, symbol: str, channel: str):
        if symbol not in self.processed_minute_data:
            """
            Client missed some subscription. Skipping those
            or Client in the ban list
            """
            return
        print(f"Unsubscription Success : {symbol} on {channel}")
        if channel == self.minute__agg_channel:
            # Do what ever you want to do before deleting the stored data
            if self.buy_sell_events.is_buy_requested() and self.buy_sell_events.get_current_bought_symbol() == symbol:
                # Sell pending sell stock immediately
                current_stamp = self.processed_minute_data[symbol][-1]['e']
                self.buy_sell_events.try_to_sell(symbol=symbol, current_timestamp=current_stamp, selling_mode="blind")
                # Delete cached data
            try:
                # print(self.processed_minute_data[symbol])
                persistant_buy_sell_data(symbol, data=self.processed_minute_data[symbol],
                                         formula_buy_sell_path=self.buy_sell_formula_path + "/final",
                                         start_date=self.processed_minute_data[symbol][0]['cal_d'],
                                         start_time=self.processed_minute_data[symbol][0]['cal_t'].replace(":", "_"),
                                         end_date=self.processed_minute_data[symbol][-1]['cal_d'],
                                         end_time=self.processed_minute_data[symbol][-1]['cal_t'].replace(":", "_"),
                                         )
            except:
                print(f"Issue on persisting {symbol} buy sell data")
            if self.buy_sell_events.ban_mode:
                if symbol in self.buy_sell_events.lost_money_on:
                    print(
                        f"[Lost Money Count Reset] {symbol} lost: [{self.buy_sell_events.lost_money_on[symbol]}] times")
                    del self.buy_sell_events.lost_money_on[symbol]
            if symbol in self.buy_sell_events.buy_commands:
                # Stop buying if trying
                del self.buy_sell_events.buy_commands[symbol]
            del self.processed_minute_data[symbol]
            del self.processed_minute_intersections[symbol]

    def is_worthy(self, symbol: str):
        """Checks if the data is worthy or not"""
        i = len(self.processed_minute_data[symbol]) - 1
        t_data = self.processed_minute_data[symbol][-1]
        if t_data['v'] > 5000:
            if (round(abs(t_data['o'] - t_data['h']), 2) > 0.02) and (
                    round(abs(t_data['h'] - t_data['l']), 2) > 0.02) and (
                    round(abs(t_data['l'] - t_data['c']), 2) > 0.02) and (
                    round(abs(t_data['c'] - t_data['o']), 2) > 0.02):
                # t_data is worthy
                worthy_count = 0
                start_from = i - 4

                if start_from < 0:
                    start_from = 0
                total_data_in_check = i - start_from + 1
                for may_unworthy in self.processed_minute_data[symbol][start_from:i + 1]:
                    if (round(abs(may_unworthy['o'] - may_unworthy['h']), 2) > 0.02) and (
                            round(abs(may_unworthy['h'] - may_unworthy['l']), 2) > 0.02) and (
                            round(abs(may_unworthy['l'] - may_unworthy['c']), 2) > 0.02) and (
                            round(abs(may_unworthy['c'] - may_unworthy['o']), 2) > 0.02):
                        worthy_count += 1
                if worthy_count >= (total_data_in_check - worthy_count):
                    return True
        return False


class WebSocketAggProvider:
    def __init__(self, key: str, uri: str):
        self.websocket_client = WebSocketClientClone(uri=f"{uri}{key}")
        self.check = True
        self.minute__agg_channel = "AM"
        self.second_agg_channel = "A"
        self.on_minute_data_received = self.on_minute_data_received_default_call
        self.on_second_data_received = self.on_second_data_received_default_call
        self.new_subscribed = self.new_subscribed_default_call
        self.new_unsubscribed = self.new_unsubscribed_default_call

    def on_second_data_received_default_call(self, second_data, symbol):
        # print(f"DEFAULT: SECOND DATA RECEIVED: {symbol} : {second_data}")
        pass

    def on_minute_data_received_default_call(self, minute_data, symbol):
        print(f"DEFAULT: MINUTE DATA RECEIVED: {symbol} : {minute_data}")

    def extract_symbol_and_channel(self, channeled_symbol: str):
        """
        Example Input: A.AAPL.B
        Returns AAPL.B, A
        Example Input: AM.ABC
        Returns ABC, AM
        """
        channeled_symbol = channeled_symbol.strip()
        dot_index = channeled_symbol.index(".")
        return channeled_symbol[dot_index + 1:], channeled_symbol[:dot_index]

    def on_data_received(self, msg):
        # print(msg)
        msg = json.loads(msg)
        for data in msg:
            if data['ev'] == self.second_agg_channel:
                self.on_second_data_received(data, data['sym'])
            elif data['ev'] == self.minute__agg_channel:
                self.on_minute_data_received(data, data['sym'])
            elif data['ev'] == "status":
                if str(data['message']).startswith("subscribed to"):
                    symbol, channel = self.extract_symbol_and_channel(data['message'].split(":")[1])
                    self.new_subscribed(symbol, channel)
                elif str(data['message']).startswith("unsubscribed to"):
                    symbol, channel = self.extract_symbol_and_channel(data['message'].split(":")[1])
                    self.new_unsubscribed(symbol, channel)

    def new_subscribed_default_call(self, symbol: str, channel: str):
        print(f"DEFAULT: Subscription Success : {symbol} on {channel}")

    def attach_new_subscribed_listener(self, new_subscribed_callable: Callable[[str, str], None]):
        """
        Transfers received data to the provided function. Given function must have one param only
        :param new_subscribed_callable: params(symbol:str, channel:str)
        :return:
        """
        self.new_subscribed = new_subscribed_callable

    def deattach_new_subscribed_listener(self):
        """
        Reset the custom added callable and sets the listener to default
        :return:
        """
        self.new_subscribed = self.new_subscribed_default_call

    def new_unsubscribed_default_call(self, symbol: str, channel: str):
        print(f"DEFAULT: Unsubscription Success : {symbol} on {channel}")

    def attach_new_unsubscribed_listener(self, new_unsubscribed_callable: Callable[[str, str], None]):
        """
        Transfers received data to the provided function. Given function must have one param only
        :param new_unsubscribed_callable: params(symbol:str, channel:str)
        :return:
        """
        self.new_unsubscribed = new_unsubscribed_callable

    def deattach_new_unsubscribed_listener(self):
        """
        Reset the custom added callable and sets the listener to default
        :return:
        """
        self.new_unsubscribed = self.new_unsubscribed_default_call

    def attach_on_minute_data_received_listener(self, on_min_callable: Callable[[dict, str], None]):
        """
        Transfers received data to the provided function. Given function must be two params
        :param on_min_callable: params(minute_data,symbol)
        :return:
        """
        self.on_minute_data_received = on_min_callable

    def deattach_on_minute_data_received_listener(self):
        """
        Reset the custom added callable and sets the listener to default
        :return:
        """
        self.on_minute_data_received = self.on_minute_data_received_default_call

    def attach_on_second_data_received_listener(self, on_sec_callable: Callable[[dict, str], None]):
        """
        Transfers received data to the provided function. Given function must be two params
        :param on_sec_callable: params(second_data,symbol)
        :return:
        """
        self.on_second_data_received = on_sec_callable

    def deattach_on_second_data_received_listener(self):
        """
        Reset the custom added callable and sets the listener to default
        :return:
        """
        self.on_second_data_received = self.on_second_data_received_default_call

    def start_fetching(self):
        self.websocket_client.attach_on_msg_listener(self.on_data_received)
        self.websocket_client.run_async()


if __name__ == '__main__':
    hours = MarketHoursCalifornia()
    print(hours.get_after_market_time_range_dict())
