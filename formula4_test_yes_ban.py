from strategy import Formula4
from trader import AlpakaTrader

alpaka_trader = AlpakaTrader()
formula1_ban_yes = Formula4(trader=alpaka_trader, socket_key='5555', socket_uri="ws://localhost:8000/api/ws/",
                            ban_mode=True)
formula1_ban_yes.start()
