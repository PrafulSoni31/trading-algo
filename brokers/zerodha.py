import os
import json
import logging
from dotenv import load_dotenv
from kiteconnect import KiteConnect, KiteTicker

# Optional helper: auto-generate token if kite_token.json is missing
try:
    from auto_login import run_auto_login
except Exception:
    run_auto_login = None

logger = logging.getLogger("zerodha")
load_dotenv()

class ZerodhaBroker:
    """Minimal Zerodha broker wrapper with token bootstrap + websocket helpers."""

    def __init__(self):
        # API creds (BROKER_* or legacy KITE_* keys)
        self.api_key = os.getenv("BROKER_API_KEY") or os.getenv("KITE_API_KEY")
        self.api_secret = os.getenv("BROKER_API_SECRET") or os.getenv("KITE_API_SECRET")
        if not self.api_key:
            raise RuntimeError("KITE_API_KEY/BROKER_API_KEY not set in .env")

        self.kite = KiteConnect(api_key=self.api_key)

        # 1) try kite_token.json (project root or brokers/../)
        access_token = None
        for p in [
            "kite_token.json",
            os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "kite_token.json")),
        ]:
            if os.path.exists(p):
                try:
                    with open(p, "r", encoding="utf-8") as f:
                        access_token = (json.load(f) or {}).get("access_token")
                        if access_token:
                            break
                except Exception as e:
                    logger.warning(f"Failed reading {p}: {e}")

        # 2) if missing, try auto_login()
        if not access_token and run_auto_login:
            logger.info("kite_token.json not found — running auto_login to fetch token...")
            run_auto_login()
            if os.path.exists("kite_token.json"):
                with open("kite_token.json", "r", encoding="utf-8") as f:
                    access_token = (json.load(f) or {}).get("access_token")

        # 3) final fallback: env
        if not access_token:
            access_token = os.getenv("KITE_ACCESS_TOKEN")
        if not access_token:
            raise RuntimeError("No access token found. Run auto_login.py or set KITE_ACCESS_TOKEN in .env")

        self.access_token = access_token
        self.kite.set_access_token(access_token)
        logger.info("Zerodha access token loaded and set.")

        self.kite_ws: KiteTicker | None = None

    # ---------- REST helpers ----------
    def get_quote(self, tradingsymbol: str) -> dict:
        return self.kite.quote([tradingsymbol])

    def historical_data(self, instrument_token: int, from_dt, to_dt, interval: str = "minute"):
        return self.kite.historical_data(instrument_token, from_dt, to_dt, interval=interval)

    def positions(self):
        return self.kite.positions()

    def orders(self):
        return self.kite.orders()

    def download_instruments(self, exchange=None, segment=None):
        """Downloads all tradable instruments from Kite, with optional filtering."""
        import pandas as pd
        logger.info(f"Downloading instruments for exchange='{exchange}' and segment='{segment}'...")
        instruments = self.kite.instruments(exchange=exchange)
        df = pd.DataFrame(instruments)

        if segment:
            df = df[df['segment'] == segment]

        self.instruments_df = df
        logger.info(f"Downloaded and filtered {len(self.instruments_df)} instruments.")
        return self.instruments_df

    # ---------- WebSocket ----------
    def connect_websocket(self, max_tries: int = 10, delay: int = 5):
        """Connects ticker in a way that works across old/new kiteconnect versions."""
        self.kite_ws = KiteTicker(self.api_key, self.access_token)

        # Bind callbacks (strategy can overwrite attributes before calling)
        self.kite_ws.on_ticks = self.on_ticks
        self.kite_ws.on_connect = self.on_connect
        self.kite_ws.on_close = self.on_close
        self.kite_ws.on_error = self.on_error
        self.kite_ws.on_reconnect = self.on_reconnect
        self.kite_ws.on_noreconnect = self.on_noreconnect

        # Try to enable auto-reconnect if the method exists; ignore otherwise
        try:
            self.kite_ws.enable_reconnect(
                reconnect=True,
                max_reconnect_tries=max_tries,
                reconnect_delay=delay,
            )
        except Exception:
            # Older builds don't have enable_reconnect — proceed without it
            pass

        # Old builds: connect() only accepts 'threaded'
        self.kite_ws.connect(threaded=True)

    # Default no-op callbacks (your strategy may override these by assignment)
    def on_ticks(self, ws, ticks): pass
    def on_connect(self, ws, response): pass
    def on_close(self, ws, code, reason): pass
    def on_error(self, ws, code, reason): pass
    def on_reconnect(self, ws, attempts_count): pass
    def on_noreconnect(self, ws): pass
