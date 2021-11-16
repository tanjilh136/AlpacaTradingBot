from stock_websocket_api import WebSocketClientClone


def test_on_msg(msg):
    """test"""
    print(msg)


if __name__ == "__main__":
    key = "121212"
    websocket_client = WebSocketClientClone(uri=f"ws://localhost:8000/api/ws/{key}")
    websocket_client.attach_on_msg_listener(test_on_msg)
    websocket_client.run_async()
    print("RUNNING")
