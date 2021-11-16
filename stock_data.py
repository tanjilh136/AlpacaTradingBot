import json
import time
from abc import ABC, abstractmethod
from threading import Thread
from typing import Callable, Optional
from creds import PolygonCreds
import requests
from data_processor import TimedStorage
import pandas as pd


class AllowedSymbols:
    def __init__(self):
        self.candidate_ticks = {symbol: True for symbol in
                                list(set(pd.read_csv("non_otc_symbols_2021_08_01.csv")["symbol"]))}

    def is_allowed_symbol(self, symbol: str):
        return symbol in self.candidate_ticks


class StockDetector(ABC):
    """Detect stock to invest. And Store the stock name for a specified number of time"""

    def __init__(self):
        self.storage = []

    @abstractmethod
    def start_detecting(self):
        """Start detecting stock"""

    @abstractmethod
    def stop_detecting(self):
        """Stop detecting stock"""


class PolygonTop20Detector(StockDetector):
    """Detect top 20 growth and filter it by target_growth in percentage"""

    def __init__(self, polygon_creds: PolygonCreds, target_growth, search_each_sec=3, validity_min=24 * 60):
        super().__init__()
        self.target_growth = target_growth
        self.carry_on = True
        self.polygon_secret_key = polygon_creds.secret_key
        self.search_each_sec = search_each_sec
        self.validity = validity_min
        self.timed_storage = TimedStorage()
        self.allowed_symbols = AllowedSymbols()
        self.t1 = Thread(target=self.start_detecting, name="growth_detector")
        self.t1.start()

    def start_detecting(self):
        """keep detecting growth after a specified interval"""
        wait_detection = 30
        print(f"Growth Detection will start withing {wait_detection}sec")
        try:
            time.sleep(wait_detection)  # Giving some time to the client to connect, so that they receive immediate data
        except:
            print("Sleeping error. Fixed")
        while self.carry_on:
            try:
                print(f"searching growth : {self.search_each_sec}")
                try:
                    time.sleep(self.search_each_sec)
                except:
                    print("Search Growth time error")
                res = requests.get(
                    f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/gainers?&apiKey={self.polygon_secret_key}")
                data = json.loads(res.text)

                if data['status'] == 'OK':
                    for d in data['tickers']:
                        if d['todaysChangePerc'] >= self.target_growth:
                            if not self.allowed_symbols.is_allowed_symbol(d['ticker']):
                                # Skip if not allowed
                                continue
                            if d['ticker'] not in self.timed_storage.get_ids():
                                print(f"GROWTH FOUND: {d['ticker']} : {d['todaysChangePerc']}")
                                self.timed_storage.push(id=d['ticker'], data=d['todaysChangePerc'],
                                                        delete_after_min=self.validity)
            except:
                print("Error on fetchin growth data")
                try:
                    print(data)
                except:
                    pass
        print(f"GROWTH DETECTION STOPPED: CARRY_ON = {self.carry_on}")

    def stop_detecting(self):
        self.carry_on = False

    def get_detected_symbols(self):
        return self.timed_storage.get_ids()

    def get_detected_id_data_and_deleted(self):
        print("GROWTH DATA PASSED")
        return self.timed_storage.get_id_and_data_and_deleted()


class MarketStream(ABC):
    """Streams realtime data of specified stock symbols"""

    @abstractmethod
    def start_stream(self, on_msg_callback: Callable, on_close_callback: Optional[Callable],
                     on_error_callback: Optional[Callable]):
        """bang"""

    @abstractmethod
    def stop_stream(self):
        """Reset stream by removing all symbols and stoping the streamer"""

    @abstractmethod
    def get_symbols(self):
        """return all the symbols that are streaming"""

    # @abstractmethod
    # def add_symbol(self, symbol: str):
    #     """Add symbol in the stream"""
    #
    # @abstractmethod
    # def remove_symbol(self, symbol: str):
    #     """Remove the specified symbol from the stream if streaming"""

    @abstractmethod
    def add_symbols(self, symbol: list):
        """Add symbols in the stream"""

    @abstractmethod
    def remove_symbols(self, symbol: list):
        """Remove the specified symbols from the stream if streaming"""


class PolygonStream(MarketStream):
    """Uses Polygon.io WebSocket to stream stockmarket data"""

    def __init__(self, polygon_creds: PolygonCreds, channel: list):
        allowed_channels = ['AM', 'T', 'A']
        for ch in channel:
            if ch not in allowed_channels:
                raise Exception(f"Channel '{channel}' is not valid")
        self.channel = [f"{ch}." for ch in channel]
        self.key = polygon_creds.secret_key  # Polygon Key

    def start_stream(self, on_msg_callback: Callable, on_close_callback: Optional[Callable] = None,
                     on_error_callback: Optional[Callable] = None):
        from polygon import WebSocketClient, STOCKS_CLUSTER
        self.my_client = WebSocketClient(cluster=STOCKS_CLUSTER, auth_key=self.key,
                                         process_message=on_msg_callback,
                                         on_close=self.closed, on_error=self.error)
        self.my_client.run_async()

    def closed(self, msg):
        print(msg)
        print("COnnection closed")

    def error(self, msg):
        print(msg)
        print("COnnection error")

    def stop_stream(self):
        self.my_client.close_connection()

    # def add_symbol(self, symbol: str):
    #     self.my_client.subscribe(f"{self.channel}{symbol}")

    # def remove_symbol(self, symbol: str):
    #     self.my_client.unsubscribe(f"{self.channel}{symbol}")

    def add_symbols(self, symbols: list):
        self.my_client.subscribe(",".join([f"{ch}{tick}" for ch in self.channel for tick in symbols]))

    def remove_symbols(self, symbols: list):
        self.my_client.unsubscribe(",".join([f"{ch}{tick}" for ch in self.channel for tick in symbols]))

    def get_symbols(self):
        return NotImplementedError


class RealTimeDataStorageForWebSocketClients:
    def __init__(self):
        self.client_ids = []
        self.client_data = {}

    async def get_data(self, client_id):
        """
        Read data asynchronously. When read complete clear the storage of that client
        :param client_id:
        :return:
        """
        res = json.dumps(self.client_data[client_id])
        self.client_data[client_id].clear()
        return res

    def store_data(self, data):
        data = json.loads(data)
        for client_id in self.client_ids:
            for one_data in data:
                self.client_data[client_id].append(one_data)

    def register_new_client(self, client_id: int):
        self.client_ids.append(client_id)
        self.client_data[client_id] = []
        print(f"Client: {client_id} connected")

    def client_disconnected(self, client_id: int):
        self.client_ids.remove(client_id)
        del self.client_data[client_id]
        print(f"Client: {client_id} disconnected")


class PolygonDataStreamMultipleClient(PolygonStream):
    def __init__(self, polygon_creds: PolygonCreds, channel: list):
        super().__init__(polygon_creds, channel)
        self.storage = None
        self.auto_sub_unsub_func = self.auto_sub_unsub
        self.current_subscribed = {}

    def attach_client_storage(self, storage: RealTimeDataStorageForWebSocketClients):
        self.storage = storage

    def on_msg(self, msg):
        print(".", end="")
        self.storage.store_data(msg)

    def auto_sub_unsub(self):
        """default callable"""
        return {}

    def keep_auto_sub_unsub(self):
        while True:
            time.sleep(3)
            print("Trying auto sub/unsub")
            res = self.auto_sub_unsub_func()
            valid = [tick['id'] for tick in res['valid']]
            expired = [tick['id'] for tick in res['expired']]
            if len(expired) > 0:
                for exp in expired:
                    try:
                        del self.current_subscribed[exp]
                    except:
                        pass

                print(f"Unsubscribing from : {expired}")
                self.remove_symbols(expired)

            if len(valid) > 0:
                new_subs = []
                for v in valid:
                    if v in self.current_subscribed:
                        continue
                    else:
                        new_subs.append(v)
                        self.current_subscribed[v] = True
                if len(new_subs) > 0:
                    print(f"Subscribing to : {new_subs}")
                    self.add_symbols(new_subs)

    def start_auto_sub_unsub(self):
        t1 = Thread(target=self.keep_auto_sub_unsub, args=[])
        t1.start()

    def attach_auto_sub_unsubscribe_callable(self, symbol_provider: Callable):
        self.auto_sub_unsub_func = symbol_provider

    def start_internal_stream(self, on_close_callback: Optional[Callable] = None,
                              on_error_callback: Optional[Callable] = None):
        from polygon import WebSocketClient, STOCKS_CLUSTER
        self.my_client = WebSocketClient(cluster=STOCKS_CLUSTER, auth_key=self.key,
                                         process_message=self.on_msg,
                                         on_close=self.on_socket_close, on_error=self.on_error_callback_default)
        self.my_client.run_async()
        self.start_auto_sub_unsub()

    def on_error_callback_default(self, msg):
        print(f"ERROR POLYGON: {msg}")

    def on_socket_close(self, socket):
        """If connection closed, restart"""
        print(">>>>>>>>>>>>>>>>>>>>>>>>ALERT_ALERT<<<<<<<<<<<<<<<<<<<<<<<<<")
        print(">>>>>>>>>>>>>>>>>>>>>>>>ALERT_ALERT<<<<<<<<<<<<<<<<<<<<<<<<<")
        print(">>>>>>>>>>>>>>>>>>POLYGON_CONNECTION_CLOSED<<<<<<<<<<<<<<<<<")
        print(">>>>>>>>>>>>>>>>>>>>>>>>ALERT_ALERT<<<<<<<<<<<<<<<<<<<<<<<<<")
        print(">>>>>>>>>>>>>>>>>>>>>>>>ALERT_ALERT<<<<<<<<<<<<<<<<<<<<<<<<<")
        # self.my_client.run_async()
        # self.add_symbols(list(self.current_subscribed.keys()))


def data_received(msg):
    print(msg)


if __name__ == "__main__":
    stream = PolygonDataStreamMultipleClient(PolygonCreds(), channel=['A', 'AM'])
    stream.start_stream(data_received)
    stream.add_symbols(["AAPL", "FLGC"])
    time.sleep(1)
    stream.remove_symbols(["AAPL", "FLGC"])
    # stream.stop_stream()
