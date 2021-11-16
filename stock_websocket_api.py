import dataclasses
import time
from threading import Thread
from typing import Callable, List
from fastapi import WebSocket, WebSocketDisconnect, FastAPI
import uvicorn
from creds import PolygonCreds
from stock_data import RealTimeDataStorageForWebSocketClients, PolygonDataStreamMultipleClient
from stock_data import PolygonTop20Detector
import websockets


class WebSocketClientClone:
    """Personal webscoket client"""

    def __init__(self, uri: str):
        self.uri = uri
        self.on_msg = self.on_msg_func

    def run_async(self):
        t1 = Thread(target=self.run, args=[])
        t1.start()

    def run(self):
        import asyncio
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self.hello(self.uri))

    def on_msg_func(self, msg):
        pass

    async def hello(self, uri: str):
        async with websockets.connect(uri) as websocket:
            while True:
                name = ""
                await websocket.send(name)
                # print(f"> {name}")
                greeting = await websocket.recv()
                if len(greeting) > 2:
                    self.on_msg(greeting)

    def attach_on_msg_listener(self, on_msg_callable: Callable):
        self.on_msg = on_msg_callable


class WebSocketConnectionManager:
    """Connection Manager for multiple websocket client"""

    def __init__(self):
        self.active_connections: List[WebSocket] = []
        self.connected_users = []

    async def connect(self, websocket_client: WebSocket, client_id: int):
        await websocket_client.accept()
        self.active_connections.append(websocket_client)
        self.connected_users.append(client_id)

    def disconnect(self, websocket_client: WebSocket, client_id: int):
        self.active_connections.remove(websocket_client)
        self.connected_users.remove(client_id)

    async def send_personal_message(self, message: str, websocket_client: WebSocket):
        await websocket_client.send_text(message)

    async def broadcast(self, message: str):
        for connection in self.active_connections:
            await connection.send_text(message)


def get_route_decorator(name):
    """Decorator provider for using in class"""

    def get_decorator(path, *args, **kwargs):
        def decorator(func):
            func.__route_args__ = (name, path, args, kwargs)
            return func

        return decorator

    return get_decorator


# create the actual decorators (could be in class of themselves)
get = get_route_decorator('get')
post = get_route_decorator('post')
put = get_route_decorator('put')
patch = get_route_decorator('patch')
websocket = get_route_decorator('websocket')


class BaseObjectApi(object):
    """ baseclass for classes that implement the endpoints """

    path = ''  # base path for all endpoints in the class

    def __init__(self, app):
        self._register(app)

    def _register(self, app):
        self.app = app
        for name in dir(self.__class__):
            method = getattr(self, name)  # bound to self
            try:
                name, path, args, kwargs = method.__route_args__  # unpack the arguments
            except AttributeError:
                pass
            else:
                new_path = self._prepend_path(path)  # prepend cls.path
                getattr(app, name)(new_path, *args, **kwargs)(
                    method)  # apply app.get(...) or app.put(...), etc. decorators

    def _prepend_path(self, path):
        return f"/{self.path.strip('/')}/{path.lstrip('/')}"


@dataclasses.dataclass
class WebSocketUsers:
    users = [1111, 2222, 3333, 4444, 5555, 6666]


class WebSocketMultipleClientServer(BaseObjectApi):
    def __init__(self, app):
        super().__init__(app)
        self.connection_manager = WebSocketConnectionManager()
        self.on_websocket_connect = self.on_websocket_con
        self.on_websocket_disconnect = self.on_websocket_discon
        self.get_client_data = self.get_client_data_async
        self.on_growth_request = self.get_new_growth_data

    path = "api"

    @get("/growth")
    async def get_growth(self):
        return self.on_growth_request()

    @websocket("/ws/{client_id}")
    async def websocket_endpoint(self, websocket_client: WebSocket, client_id: int):
        if client_id not in WebSocketUsers.users:
            return

        await self.connection_manager.connect(websocket_client, client_id)
        self.on_websocket_connect(client_id)
        try:
            while True:
                msg_received = await websocket_client.receive_text()  # clients replys when msg received
                client_data = await self.get_client_data(client_id)
                await self.connection_manager.send_personal_message(client_data, websocket_client)
        except WebSocketDisconnect:
            self.connection_manager.disconnect(websocket_client, client_id)
            self.on_websocket_disconnect(client_id)

    def get_new_growth_data(self):
        return "GROWTH WORKING"

    def on_websocket_discon(self, client_id: int):
        pass

    def on_websocket_con(self, client_id: int):
        pass

    async def get_client_data_async(self, client_id: int):
        """
        Default test get_client_data callable function
        :param client_id:
        :return:
        """
        return "test data string"

    def attach_on_growth_request_callable(self, growth_callable):
        self.on_growth_request = growth_callable

    def attach_on_websocket_discon_callable(self, discon_callable: Callable):
        """

        :param discon_callable: params (client_id)
        :return:
        """
        self.on_websocket_disconnect = discon_callable

    def attach_on_websocket_con_callable(self, con_callable: Callable):
        """
        :param con_callable: params (client_id)
        :return:
        """
        self.on_websocket_connect = con_callable

    def attach_client_data_provider_callable(self, client_data_provider: Callable):
        """
        :param client_data_provider: return data. params (client_id)
        :return:
        """
        self.get_client_data = client_data_provider

    def start(self):
        t1 = Thread(target=uvicorn.run, args=[self.app])
        t1.start()


def main():
    stream_data = PolygonDataStreamMultipleClient(PolygonCreds(), channel=["A", "AM"])
    storage = RealTimeDataStorageForWebSocketClients()
    stream_data.attach_client_storage(storage)
    stream_data.start_internal_stream()
    app = WebSocketMultipleClientServer(app=FastAPI())
    symbol_detector = PolygonTop20Detector(PolygonCreds(), target_growth=16, search_each_sec=10, validity_min=60)
    stream_data.attach_auto_sub_unsubscribe_callable(symbol_detector.get_detected_id_data_and_deleted)
    app.attach_on_growth_request_callable(symbol_detector.get_detected_id_data_and_deleted)
    app.attach_on_websocket_con_callable(storage.register_new_client)
    app.attach_on_websocket_discon_callable(storage.client_disconnected)
    app.attach_client_data_provider_callable(storage.get_data)
    app.start()
    # time.sleep(20)
    # stream_data.add_symbols(['AAPL'])


if __name__ == "__main__":
    main()
