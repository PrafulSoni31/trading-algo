import os
import sys
from datetime import datetime, timedelta
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
        self._oi_buffers = defaultdict(deque)   # token -> deque[(ts, oi)]
        self._buf_maxlen = 2000                 # ~enough for a few hours at 1s-5s ticks

        # token metadata -> (strike, type) to format the tables easily
        self._token_meta = {}                   # token -> {"strike": int|None, "itype": "CE"/"PE"/"IDX"}

        self.historical_data_dfs = {}
        try:
            pygame.mixer.init()
        except Exception:
            pass

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
        now = datetime.now()

        # 1) ingest ticks
        updated_tokens = set()
        for tk in ticks:
            token = int(tk.get("instrument_token"))
            oi = tk.get("oi")
            if oi is None:
                # index LTP tick will not have OI; skip
                continue
            self._live_oi[token] = int(oi)
            buf = self._oi_buffers[token]
            buf.append((now, int(oi)))
            if len(buf) > self._buf_maxlen:
                buf.popleft()
            updated_tokens.add(token)

        if not updated_tokens:
            return  # nothing to aggregate

        # 2) ensure minute history tracks along
        for token in updated_tokens:
            df = self.historical_data_dfs.get(token)
            if df is None or df.empty:
                self.historical_data_dfs[token] = pd.DataFrame({"oi": [self._live_oi[token]]}, index=[now])
            else:
                last_idx = df.index[-1] if len(df.index) else None
                if last_idx is None or (now - last_idx).total_seconds() >= 55:
                    # If 'oi' column isn't there yet, create it
                    if "oi" not in df.columns:
                        df["oi"] = None
                    df.loc[now, "oi"] = self._live_oi[token]
                    cutoff = now - timedelta(hours=4)
                    self.historical_data_dfs[token] = df[df.index >= cutoff]

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

        for token, latest_oi in self._live_oi.items():
            meta = self._token_meta.get(token) or {}
            itype = meta.get("itype")
            strike = meta.get("strike")
            if itype not in ("CE", "PE"):
                continue  # show only options

            row = {
                "Strike": strike,
                "Type": itype,
                "Current OI": latest_oi,
            }

            for label, delta_t in windows.items():
                past_oi = _oi_at_or_before(token, now - delta_t)
                row[label] = (latest_oi - past_oi) if (past_oi is not None) else None

            rows.append(row)

        if not rows:
            return

        df_rows = pd.DataFrame(rows)
        # Coerce deltas to numeric, treat missing as 0 so .abs() won’t fail
        for col in ["3 Min","5 Min","10 Min","15 Min","30 Min","3 Hr"]:
            if col in df_rows.columns:
                df_rows[col] = pd.to_numeric(df_rows[col], errors="coerce").fillna(0)

        # Stable sort by 5m then 15m magnitude
        df_rows["_sort"] = df_rows["5 Min"].abs()
        df_rows["_sort2"] = df_rows["15 Min"].abs()
        df_rows.sort_values(
            by=["_sort", "_sort2", "Strike", "Type"],
            ascending=[False, False, True, True],
            inplace=True
        )
        df_rows.drop(columns=["_sort", "_sort2"], inplace=True)

        # pretty print top 10 compact
        to_show = df_rows.head(10).copy()
        for col in ["3 Min","5 Min","10 Min","15 Min","30 Min","3 Hr"]:
            if col in to_show.columns:
                to_show[col] = to_show[col].apply(lambda v: "" if v is None else int(v))

        if not hasattr(self, "_last_print") or (now - getattr(self, "_last_print")).total_seconds() > 10:
            logger.info("OI d (top by 5m): Strike  Type | Cur  d3m  d5m  d10m  d15m  d30m  d3h")
            self._last_print = now

        for _, r in to_show.iterrows():
            logger.info(
                f"OI d: {str(r['Strike']).rjust(6)}  {r['Type']:>3} | "
                f"{str(r['Current OI']).rjust(6)}  "
                f"{str(r.get('3 Min','')).rjust(4)}  "
                f"{str(r.get('5 Min','')).rjust(4)}  "
                f"{str(r.get('10 Min','')).rjust(5)}  "
                f"{str(r.get('15 Min','')).rjust(5)}  "
                f"{str(r.get('30 Min','')).rjust(5)}  "
                f"{str(r.get('3 Hr','')).rjust(5)}"
            )

        # Optional: save snapshot every 60s
        # if getattr(self, "_last_csv_at", None) is None or (now - self._last_csv_at).total_seconds() >= 60:
        #     snap_path = os.path.join(PROJECT_ROOT, "oi_snapshot.csv")
        #     df_rows.sort_values(["Strike","Type"], inplace=True)
        #     df_rows.to_csv(snap_path, index=False)
        #     self._last_csv_at = now
        #     logger.info(f"Saved OI snapshot -> {snap_path}")

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
