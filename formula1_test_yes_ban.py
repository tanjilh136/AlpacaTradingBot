from creds import AlpakaCreds
from strategy import Formula1
from trader import AlpakaTrader

alpaka_trader = AlpakaTrader().set_credentials(AlpakaCreds())
formula1_ban_yes = Formula1(trader=alpaka_trader, socket_key='1111', socket_uri="ws://localhost:8000/api/ws/",
                            ban_mode=True)
formula1_ban_yes.start()
