from creds import AlpakaCreds
from strategy import Formula1
from trader import AlpakaTrader

alpaka_trader = AlpakaTrader().set_credentials(AlpakaCreds())
formula1_ban_no = Formula1(trader=alpaka_trader, socket_key='2222', socket_uri="ws://localhost:8000/api/ws/",
                            ban_mode=False)
formula1_ban_no.start()