from strategy import Formula3
from creds import AlpakaCreds2
from trader import AlpakaTrader

alpaka_trader = AlpakaTrader().set_credentials(AlpakaCreds2())
formula3_ban_no = Formula3(trader=alpaka_trader, socket_key='4444', socket_uri="ws://localhost:8000/api/ws/",
                           ban_mode=False)
formula3_ban_no.start()
