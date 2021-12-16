# import asyncio
# import websockets
# def on_msg(msg):
#     print(msg)
# async def hello():
#     uri = "ws://localhost:8000/api/ws/2"
#     async with websockets.connect(uri) as websocket:
#          while True:
#             name = ""
#             await websocket.send(name)
#             # print(f"> {name}")
#             greeting = await websocket.recv()
#             if len(greeting)>2:
#                 on_msg(greeting)
#
# asyncio.get_event_loop().run_until_complete(hello())
# print("FF")
def test_on_msg(msg):
    """Test"""
    print(msg)


from stock_websocket_api import WebSocketClientClone

if __name__ == "__main__":
    key = ""
    websocket_client = WebSocketClientClone(uri=f"ws://localhost:8000/api/ws/{key}")
    websocket_client.attach_on_msg_listener(test_on_msg)
    websocket_client.run_async()
    print("RUNNING")
