from strategy import Formula4
from trader import AlpakaTrader

alpaka_trader = AlpakaTrader()
formula1_ban_no = Formula4(trader=alpaka_trader, socket_key='6666', socket_uri="ws://localhost:8000/api/ws/",
                            ban_mode=False)
formula1_ban_no.start()
