#oi_tracker.py
import os
import sys
from datetime import datetime, timedelta, timezone
from queue import Queue
from collections import deque, defaultdict

# --- Make project root importable ---
CURR_DIR = os.path.dirname(__file__)
PROJECT_ROOT = os.path.abspath(os.path.join(CURR_DIR, ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import yaml
import pandas as pd
import pygame
from termcolor import colored
from logger import logger
from dispatcher import DataDispatcher
from brokers.zerodha import ZerodhaBroker

CONFIG_PATH = os.path.join(CURR_DIR, "configs", "oi_tracker.yml")

def _to_int(x):
    """Ensure plain Python int for Kite ticker methods."""
    try:
        return int(x)
    except Exception:
        return int(float(x))

class OITrackerStrategy:
    """Tracks OI deltas across selected NIFTY option strikes."""
    def __init__(self, broker: ZerodhaBroker, cfg: dict):
        self.broker = broker
        self.cfg = cfg
        self.exchange = cfg.get("exchange", "NFO")
        self.symbol = cfg.get("symbol", "NIFTY")
        self.strike_difference = float(cfg.get("strike_difference", 50))
        self.lot_size = int(cfg.get("lot_size", 50))
        self.refresh_interval = int(cfg.get("refresh_interval", 1))

        instruments_csv = cfg.get("instruments_csv", os.path.join(PROJECT_ROOT, "instruments.csv"))
        if not os.path.exists(instruments_csv):
            raise FileNotFoundError(
                f"Missing instruments file: {instruments_csv}. "
                f"Set 'instruments_csv' in {CONFIG_PATH}"
            )
        self.instruments = pd.read_csv(instruments_csv)

        logger.info(f"Strike difference for is {self.strike_difference}")

        # live OI store per token and rolling buffer of (timestamp, oi)
        self._live_oi = {}                      # token -> latest OI
        self._live_nifty_price = 24000.0        # Default NIFTY price until first tick
        self._oi_buffers = defaultdict(deque)   # token -> deque[(ts, oi)]
        self._buf_maxlen = 2000                 # ~enough for a few hours at 1s-5s ticks

        # token metadata -> (strike, type) to format the tables easily
        self._token_meta = {}                   # token -> {"strike": int|None, "itype": "CE"/"PE"/"IDX"}

        self.historical_data_dfs = {}
        try:
            pygame.mixer.init()
        except Exception:
            pass

        # Initialize the three tables
        self.put_oi_data = pd.DataFrame(columns=['Strike', 'Current OI', '3 Min', '5 Min', '10 Min', '15 Min', '30 Min', '3 Hr'])
        self.call_oi_data = pd.DataFrame(columns=['Strike', 'Current OI', '3 Min', '5 Min', '10 Min', '15 Min', '30 Min', '3 Hr'])
        self.nifty_data = pd.DataFrame(columns=['Current NIFTY', '3 Min', '5 Min', '10 Min', '15 Min', '30 Min', '3 Hr'])


        self._prepare_history_and_subscriptions()

    def _prepare_history_and_subscriptions(self):
        now = datetime.now()
        from_date = now - timedelta(hours=3)

        # --- NIFTY 50 token ---
        nifty_df = self.instruments[self.instruments['tradingsymbol'] == 'NIFTY 50']
        if nifty_df.empty:
            raise RuntimeError("NIFTY 50 instrument not found in instruments.csv")
        self._nifty_token = _to_int(nifty_df.iloc[0]['instrument_token'])

        # --- ATM ±2 strikes ---
        try:
            current_price = self.broker.get_quote("NSE:NIFTY 50")['NSE:NIFTY 50']['last_price']
        except Exception as e:
            logger.error(f"Could not fetch initial NIFTY price: {e}")
            current_price = float(nifty_df.iloc[0].get('last_price', 24000.0))

        atm = self._get_atm_strike(current_price)
        strikes = [atm + i * self.strike_difference for i in range(-2, 3)]

        tokens = [self._nifty_token]

        for strike in strikes:
            pe = self.instruments[
                (self.instruments['strike'] == strike) &
                (self.instruments['instrument_type'] == 'PE')
            ]
            ce = self.instruments[
                (self.instruments['strike'] == strike) &
                (self.instruments['instrument_type'] == 'CE')
            ]
            if not pe.empty:
                tokens.append(_to_int(pe.iloc[0]['instrument_token']))
            if not ce.empty:
                tokens.append(_to_int(ce.iloc[0]['instrument_token']))

        self._option_tokens = [t for t in tokens if t != self._nifty_token]

        # --- Build token metadata for quick lookup in on_ticks_update ---
        self._token_meta[self._nifty_token] = {"strike": None, "itype": "IDX"}
        for t in self._option_tokens:
            row = self.instruments[self.instruments["instrument_token"] == t]
            strike = None
            itype = None
            if not row.empty:
                try:
                    strike = int(float(row.iloc[0].get("strike", 0)))
                except Exception:
                    strike = None
                itype = row.iloc[0].get("instrument_type") or None
            self._token_meta[int(t)] = {"strike": strike, "itype": itype}

        # --- History preload ---
        all_needed = [self._nifty_token] + self._option_tokens
        for token in all_needed:
            try:
                df = pd.DataFrame(self.broker.historical_data(token, from_date, now, "minute"))
            except Exception as e:
                logger.error(f"historical_data failed for token {token}: {e}")
                df = pd.DataFrame()
            if not df.empty:
                df['date'] = pd.to_datetime(df['date'])
                df.set_index('date', inplace=True)
                # Keep only OI if present; otherwise create it later
                cols = df.columns
                if "oi" not in cols and "oi" not in df:
                    # make a single-column df to append to later
                    df = df[[]]
                self.historical_data_dfs[_to_int(token)] = df

        logger.info("Initial historical data populated.")

    def _get_atm_strike(self, price: float) -> int:
        return int(round(price / self.strike_difference) * self.strike_difference)

    def on_ticks_update(self, ticks):
        """
        Consume incoming FULL/LTP ticks, update live OI, extend minute history, and
        print OI deltas for CE/PE grouped by strike over time windows.
        """
        now = datetime.now(timezone.utc)

        # 1) ingest ticks
        updated_oi_tokens = set()
        for tk in ticks:
            token = int(tk.get("instrument_token"))
            if token == self._nifty_token:
                # It's an LTP tick for NIFTY
                price = tk.get("last_price")
                if price is not None:
                    self._live_nifty_price = float(price)
            else:
                # It's a FULL tick for an option
                oi = tk.get("oi")
                if oi is None:
                    continue
                self._live_oi[token] = int(oi)
                buf = self._oi_buffers[token]
                buf.append((now, int(oi)))
                if len(buf) > self._buf_maxlen:
                    buf.popleft()
                updated_oi_tokens.add(token)

        # 2) ensure minute history tracks along
        for token in updated_oi_tokens:
            df = self.historical_data_dfs.get(token)
            if df is None or df.empty:
                self.historical_data_dfs[token] = pd.DataFrame({"oi": [self._live_oi[token]]}, index=[now])
            else:
                last_idx = df.index[-1] if len(df.index) else None
                if last_idx is None or (now - last_idx).total_seconds() >= 55:
                    if "oi" not in df.columns:
                        df["oi"] = None # Add oi column if not present
                    df.loc[now, "oi"] = self._live_oi[token]
                    cutoff = now - timedelta(hours=4)
                    self.historical_data_dfs[token] = df[df.index >= cutoff]

        # also update nifty history
        nifty_df = self.historical_data_dfs.get(self._nifty_token)
        if nifty_df is not None:
            last_idx = nifty_df.index[-1] if len(nifty_df.index) else None
            if last_idx is None or (now - last_idx).total_seconds() >= 55:
                nifty_df.loc[now, "close"] = self._live_nifty_price
                cutoff = now - timedelta(hours=4)
                self.historical_data_dfs[self._nifty_token] = nifty_df[nifty_df.index >= cutoff]

        # 3) compute deltas for windows using history where possible
        windows = {
            "3 Min": timedelta(minutes=3),
            "5 Min": timedelta(minutes=5),
            "10 Min": timedelta(minutes=10),
            "15 Min": timedelta(minutes=15),
            "30 Min": timedelta(minutes=30),
            "3 Hr": timedelta(hours=3),
        }

        rows = []

        def _oi_at_or_before(token: int, ts: datetime) -> int | None:
            df = self.historical_data_dfs.get(token)
            if df is None or df.empty or "oi" not in df.columns:
                return None
            sub = df.loc[:ts]
            if sub.empty or "oi" not in sub:
                return None
            v = sub["oi"].dropna()
            if v.empty:
                return None
            return int(v.iloc[-1])

        def _price_at_or_before(token: int, ts: datetime) -> float | None:
            df = self.historical_data_dfs.get(token)
            if df is None or df.empty or "close" not in df.columns:
                return None
            sub = df.loc[:ts]
            if sub.empty or "close" not in sub:
                return None
            v = sub["close"].dropna()
            if v.empty:
                return None
            return float(v.iloc[-1])

        current_price = self._live_nifty_price
        atm_strike = self._get_atm_strike(current_price)
        strikes = [atm_strike + i * self.strike_difference for i in range(-2, 3)]

        # Clear the tables before populating
        self.put_oi_data = pd.DataFrame(columns=self.put_oi_data.columns)
        self.call_oi_data = pd.DataFrame(columns=self.call_oi_data.columns)
        self.nifty_data = pd.DataFrame(columns=self.nifty_data.columns)

        for strike in strikes:
            # Find the put and call tokens for the current strike
            pe_token = next((t for t, m in self._token_meta.items() if m['strike'] == strike and m['itype'] == 'PE'), None)
            ce_token = next((t for t, m in self._token_meta.items() if m['strike'] == strike and m['itype'] == 'CE'), None)

            # --- Populate Put Table ---
            if pe_token and pe_token in self._live_oi:
                latest_oi = self._live_oi[pe_token]
                row = {"Strike": strike, "Current OI": latest_oi}
                for label, delta_t in windows.items():
                    past_oi = _oi_at_or_before(pe_token, now - delta_t)
                    row[label] = (latest_oi, past_oi) # Store tuple for later processing
                self.put_oi_data.loc[strike] = row

            # --- Populate Call Table ---
            if ce_token and ce_token in self._live_oi:
                latest_oi = self._live_oi[ce_token]
                row = {"Strike": strike, "Current OI": latest_oi}
                for label, delta_t in windows.items():
                    past_oi = _oi_at_or_before(ce_token, now - delta_t)
                    row[label] = (latest_oi, past_oi) # Store tuple for later processing
                self.call_oi_data.loc[strike] = row

        # --- Populate NIFTY Table ---
        nifty_row = {"Current NIFTY": current_price}
        for label, delta_t in windows.items():
            past_price = _price_at_or_before(self._nifty_token, now - delta_t)
            nifty_row[label] = (current_price, past_price)
        self.nifty_data.loc[0] = nifty_row

        # NOTE: The rest of the original function that prints tables is now obsolete
        # and will be replaced by the _print_tables and _check_alerts methods.
        # This part of the code will be removed in a later step.

        # Now apply the formatting to the tables
        self._apply_formatting()

        # Print the tables
        self._print_tables()

        # Check for alerts
        self._check_alerts()

    def _format_cell(self, value_tuple):
        """Formats a (current, past) tuple into the desired string format."""
        if not isinstance(value_tuple, tuple) or len(value_tuple) != 2:
            return "N/A"

        current, past = value_tuple
        if past is None or past == 0:
            return "N/A"

        absolute_change = current - past
        percent_change = (absolute_change / past) * 100

        return f"{percent_change:.2f}% ({absolute_change})"

    def _apply_formatting(self):
        """Applies the cell formatting to all three tables."""
        for col in ['3 Min', '5 Min', '10 Min', '15 Min', '30 Min', '3 Hr']:
            if col in self.put_oi_data.columns:
                self.put_oi_data[col] = self.put_oi_data[col].apply(self._format_cell)
            if col in self.call_oi_data.columns:
                self.call_oi_data[col] = self.call_oi_data[col].apply(self._format_cell)
            if col in self.nifty_data.columns:
                self.nifty_data[col] = self.nifty_data[col].apply(self._format_cell)
        # The old printing logic is now removed.

    def _is_red(self, val, col_name):
        if isinstance(val, str) and '%' in val:
            try:
                percent = float(val.split('%')[0])
                time_val, time_unit = col_name.split(' ')
                time_val = int(time_val)

                thresholds = self.cfg.get("color_thresholds", {})

                if time_unit == 'Hr':
                    time_val = time_val * 60

                if str(time_val) in thresholds and percent > thresholds[str(time_val)]:
                    return True

            except (ValueError, IndexError):
                pass
        return False

    def _print_tables(self):

        def color_code_df(df):
            df_colored = df.copy()
            for col in ['3 Min', '5 Min', '10 Min', '15 Min', '30 Min', '3 Hr']:
                if col in df_colored.columns:
                    df_colored[col] = df_colored[col].apply(lambda x: colored(x, 'red') if self._is_red(x, col) else x)
            return df_colored

        # Clear console before printing
        os.system('cls' if os.name == 'nt' else 'clear')

        print("--- Put OI Data ---")
        print(color_code_df(self.put_oi_data).to_string())

        print("\n--- Call OI Data ---")
        print(color_code_df(self.call_oi_data).to_string())

        print("\n--- NIFTY Data ---")
        print(self.nifty_data.to_string())

    def _check_alerts(self):

        def count_red_cells(df):
            count = 0
            for col in df.columns:
                if 'Min' in col or 'Hr' in col:
                    for val in df[col]:
                        if self._is_red(val, col):
                            count += 1
            return count

        put_red_cells = count_red_cells(self.put_oi_data)
        call_red_cells = count_red_cells(self.call_oi_data)

        total_cells_per_table = (len(self.put_oi_data.index) * 6) # 6 time columns

        if total_cells_per_table > 0 and ((put_red_cells / total_cells_per_table) > 0.3 or (call_red_cells / total_cells_per_table) > 0.3):
            try:
                alert_sound = self.cfg.get("alert_sound", "alert.wav")
                pygame.mixer.music.load(alert_sound)
                pygame.mixer.music.play()
            except pygame.error:
                logger.error(f"Could not play alert sound. Make sure '{alert_sound}' is in the root directory.")
            logger.warning("ALERT: More than 30% of cells in one of the tables are red!")


def main():
    # Load YAML config
    with open(CONFIG_PATH, "r") as f:
        cfg = yaml.safe_load(f)

    broker = ZerodhaBroker()
    dispatcher = DataDispatcher()
    dispatcher.register_main_queue(Queue())

    strategy = OITrackerStrategy(broker, cfg)

    # --- Callbacks wired to this strategy instance ---
    def on_ticks(ws, ticks):
        try:
            dispatcher.dispatch(ticks)
        except Exception as e:
            logger.error(f"Dispatch error: {e}", exc_info=True)

    def on_connect(ws, response):
        try:
            logger.info(f"Websocket connected successfully: {response}")
            ntok = _to_int(strategy._nifty_token)
            logger.info(f"Subscribing to NIFTY 50 (token: {ntok})...")
            ws.subscribe([ntok])
            logger.info("NIFTY 50 subscription successful.")
            ws.set_mode(ws.MODE_LTP, [ntok])
            logger.info("NIFTY 50 mode set to LTP.")

            option_tokens = [_to_int(t) for t in strategy._option_tokens]
            if option_tokens:
                logger.info(f"Subscribing to {len(option_tokens)} option instruments...")
                ws.subscribe(option_tokens)
                logger.info("Option instruments subscription successful.")
                ws.set_mode(ws.MODE_FULL, option_tokens)
                logger.info("Option instruments mode set to FULL.")
        except Exception as e:
            logger.error(f"on_connect failed: {e}", exc_info=True)

    broker.on_ticks = on_ticks
    broker.on_connect = on_connect

    broker.connect_websocket()

    try:
        while True:
            try:
                tick_data = dispatcher._main_queue.get()
                strategy.on_ticks_update(tick_data)
            except KeyboardInterrupt:
                logger.info("KeyboardInterrupt received. Stopping strategy.")
                break
            except Exception as e:
                logger.error(f"Error processing tick data: {e}", exc_info=True)
                continue
    except Exception:
        logger.error("FATAL ERROR in main loop", exc_info=True)
    finally:
        logger.info("STRATEGY SHUTDOWN COMPLETE")

if __name__ == "__main__":
    main()
