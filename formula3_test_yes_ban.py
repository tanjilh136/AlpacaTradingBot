from strategy import Formula3
from trader import AlpakaTrader

alpaka_trader = AlpakaTrader()
formula3_ban_yes = Formula3(trader=alpaka_trader, socket_key='3333', socket_uri="ws://localhost:8000/api/ws/",
                            ban_mode=True)
formula3_ban_yes.start()