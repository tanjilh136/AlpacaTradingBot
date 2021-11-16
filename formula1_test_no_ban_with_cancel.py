from creds import AlpakaCreds2
from strategy import Formula1
from trader import AlpakaTrader

alpaka_trader = AlpakaTrader().set_credentials(AlpakaCreds2())
formula1_ban_no = Formula1(trader=alpaka_trader, socket_key='1111', socket_uri="ws://localhost:8000/api/ws/",
                           ban_mode=False, with_cancel=True)
formula1_ban_no.start()
