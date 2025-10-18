import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import yaml
from logger import logger

import pandas as pd
import time
from datetime import datetime, timedelta
import pygame
from termcolor import colored

class OITrackerStrategy:
    """
    OI Tracker Strategy

    This strategy tracks the change in Open Interest (OI) for ATM and 2 slightly ITM and 2 slightly OTM options,
    for both call and put options.
    """

    def __init__(self, broker, config):
        # Assign config values as instance variables with 'strat_var_' prefix
        for k, v in config.items():
            setattr(self, f'strat_var_{k}', v)
        # External dependencies
        self.broker = broker
        self.symbol_initials = self.strat_var_symbol_initials
        self.broker.download_instruments()
        self.instruments = self.broker.instruments_df[self.broker.instruments_df['tradingsymbol'].str.startswith(self.symbol_initials)]   # For Zerodha
        if self.instruments.shape[0] == 0:
            logger.error(f"No instruments found for {self.symbol_initials}")
            logger.error(f"Instument {self.symbol_initials} not found. Please check the symbol initials")
            return

        self.strike_difference = None

        # Calculate and store strike difference for the option series
        self.strike_difference = self._get_strike_difference(self.symbol_initials)
        logger.info(f"Strike difference for {self.symbol_initials} is {self.strike_difference}")

        self._initialize_tables()
        pygame.mixer.init()
        self.last_update_time = None
        self.historical_data_dfs = {}
        self._populate_historical_data()

    def _populate_historical_data(self):
        now = datetime.now()
        from_date = now - timedelta(hours=3)

        nifty_instrument = self.instruments[self.instruments['tradingsymbol'] == 'NIFTY 50'].iloc[0]
        all_instruments = [nifty_instrument]

        # This is a bit inefficient to get the ATM strike here, but we need it to get the relevant instruments.
        # This assumes the app starts during market hours. A more robust solution would handle this better.
        try:
            current_price = self.broker.get_quote("NSE:NIFTY 50")['NSE:NIFTY 50']['last_price']
        except Exception as e:
            logger.error(f"Could not fetch initial NIFTY price: {e}")
            # Fallback to a reasonable default if market is closed, etc.
            current_price = self.instruments['strike'].median()

        atm_strike = self._get_atm_strike(current_price)
        strike_prices = [atm_strike + i * self.strike_difference for i in range(-2, 3)]

        for strike in strike_prices:
            put_instrument = self.instruments[(self.instruments['strike'] == strike) & (self.instruments['instrument_type'] == 'PE')]
            call_instrument = self.instruments[(self.instruments['strike'] == strike) & (self.instruments['instrument_type'] == 'CE')]
            if not put_instrument.empty:
                all_instruments.append(put_instrument.iloc[0])
            if not call_instrument.empty:
                all_instruments.append(call_instrument.iloc[0])

        for instrument in all_instruments:
            token = instrument['instrument_token']
            df = pd.DataFrame(self.broker.historical_data(token, from_date, now, "minute"))
            if not df.empty:
                df['date'] = pd.to_datetime(df['date'])
                df.set_index('date', inplace=True)
                self.historical_data_dfs[token] = df

        logger.info("Initial historical data populated.")

    def _initialize_tables(self):
        self.put_oi_data = pd.DataFrame(columns=['Strike', 'Current OI', '3 Min', '5 Min', '10 Min', '15 Min', '30 Min', '3 Hr'])
        self.call_oi_data = pd.DataFrame(columns=['Strike', 'Current OI', '3 Min', '5 Min', '10 Min', '15 Min', '30 Min', '3 Hr'])
        self.nifty_data = pd.DataFrame(columns=['Current NIFTY', '3 Min', '5 Min', '10 Min', '15 Min', '30 Min', '3 Hr'])

    def _get_strike_difference(self, symbol_initials):
        if self.strike_difference is not None:
            return self.strike_difference

        # Filter for CE instruments to calculate strike difference
        ce_instruments = self.instruments[
            self.instruments['tradingsymbol'].str.startswith(symbol_initials) &
            self.instruments['tradingsymbol'].str.endswith('CE')
        ]

        if ce_instruments.shape[0] < 2:
            logger.error(f"Not enough CE instruments found for {symbol_initials} to calculate strike difference")
            return 0
        # Sort by strike
        ce_instruments_sorted = ce_instruments.sort_values('strike')
        # Take the top 2
        top2 = ce_instruments_sorted.head(2)
        # Calculate the difference
        self.strike_difference = abs(top2.iloc[1]['strike'] - top2.iloc[0]['strike'])
        return self.strike_difference

    def _get_atm_strike(self, current_price):
        return round(current_price / self.strike_difference) * self.strike_difference

    def _get_historical_oi(self, instrument_token, from_date, to_date):
        try:
            records = self.broker.historical_data(instrument_token, from_date, to_date, "minute")
            if records:
                return records[-1]['oi']
            return 0
        except Exception as e:
            logger.error(f"Error fetching historical OI for {instrument_token}: {e}")
            return 0

    def on_ticks_update(self, ticks):
        """
        Main strategy execution method called on each tick update

        Args:
            ticks (dict): Market data containing 'last_price' and other tick information
        """

        now = datetime.now()
        if self.last_update_time and (now - self.last_update_time).total_seconds() < 60:
            return

        # Align to the minute
        if self.last_update_time and self.last_update_time.minute == now.minute:
            return

        self.last_update_time = now

        # Fetch last minute's candle for all instruments and append to our historical data
        to_date = now.replace(second=0, microsecond=0)
        from_date = to_date - timedelta(minutes=1)
        for token in self.historical_data_dfs.keys():
            try:
                new_candle = self.broker.historical_data(token, from_date, to_date, "minute")
                if new_candle:
                    new_df = pd.DataFrame(new_candle)
                    new_df['date'] = pd.to_datetime(new_df['date'])
                    new_df.set_index('date', inplace=True)
                    self.historical_data_dfs[token] = pd.concat([self.historical_data_dfs[token], new_df])
                    # Prune old data to keep the DataFrame size manageable
                    self.historical_data_dfs[token] = self.historical_data_dfs[token].last('3H')
            except Exception as e:
                logger.error(f"Could not update historical data for token {token}: {e}")

        nifty_instrument = self.instruments[self.instruments['tradingsymbol'] == 'NIFTY 50'].iloc[0]
        nifty_df = self.historical_data_dfs.get(nifty_instrument['instrument_token'])
        if nifty_df is None or nifty_df.empty:
            return

        current_price = nifty_df['close'].iloc[-1]
        atm_strike = self._get_atm_strike(current_price)

        strike_prices = [atm_strike + i * self.strike_difference for i in range(-2, 3)]

        for strike in strike_prices:
            put_instrument = self.instruments[(self.instruments['strike'] == strike) & (self.instruments['instrument_type'] == 'PE')]
            call_instrument = self.instruments[(self.instruments['strike'] == strike) & (self.instruments['instrument_type'] == 'CE')]

            if not put_instrument.empty:
                put_instrument_token = put_instrument.iloc[0]['instrument_token']
                put_df = self.historical_data_dfs.get(put_instrument_token)
                if put_df is not None and not put_df.empty:
                    current_put_oi = put_df['oi'].iloc[-1]
                    self.put_oi_data.loc[strike, 'Strike'] = strike
                    self.put_oi_data.loc[strike, 'Current OI'] = current_put_oi

                    for t in [3, 5, 10, 15, 30, 180]:
                        past_time = now - timedelta(minutes=t)
                        past_oi_series = put_df['oi'].asof(past_time)
                        if not pd.isna(past_oi_series) and past_oi_series > 0:
                            change_percent = ((current_put_oi - past_oi_series) / past_oi_series) * 100
                            change_absolute = current_put_oi - past_oi_series
                            self.put_oi_data.loc[strike, f'{t} Min' if t < 60 else f'{t//60} Hr'] = f"{change_percent:.2f}% ({change_absolute})"
                        else:
                            self.put_oi_data.loc[strike, f'{t} Min' if t < 60 else f'{t//60} Hr'] = "N/A"

            if not call_instrument.empty:
                call_instrument_token = call_instrument.iloc[0]['instrument_token']
                call_df = self.historical_data_dfs.get(call_instrument_token)
                if call_df is not None and not call_df.empty:
                    current_call_oi = call_df['oi'].iloc[-1]
                    self.call_oi_data.loc[strike, 'Strike'] = strike
                    self.call_oi_data.loc[strike, 'Current OI'] = current_call_oi

                    for t in [3, 5, 10, 15, 30, 180]:
                        past_time = now - timedelta(minutes=t)
                        past_oi_series = call_df['oi'].asof(past_time)
                        if not pd.isna(past_oi_series) and past_oi_series > 0:
                            change_percent = ((current_call_oi - past_oi_series) / past_oi_series) * 100
                            change_absolute = current_call_oi - past_oi_series
                            self.call_oi_data.loc[strike, f'{t} Min' if t < 60 else f'{t//60} Hr'] = f"{change_percent:.2f}% ({change_absolute})"
                        else:
                            self.call_oi_data.loc[strike, f'{t} Min' if t < 60 else f'{t//60} Hr'] = "N/A"

        self.nifty_data.loc[0, 'Current NIFTY'] = current_price
        for t in [3, 5, 10, 15, 30, 180]:
            past_time = now - timedelta(minutes=t)
            past_price_series = nifty_df['close'].asof(past_time)
            if not pd.isna(past_price_series):
                past_price = past_price_series
                change_percent = ((current_price - past_price) / past_price) * 100
                change_absolute = current_price - past_price
                self.nifty_data.loc[0, f'{t} Min' if t < 60 else f'{t//60} Hr'] = f"{change_percent:.2f}% ({change_absolute})"
            else:
                self.nifty_data.loc[0, f'{t} Min' if t < 60 else f'{t//60} Hr'] = "N/A"

        self._print_tables()
        self._check_alerts()

    def _is_red(self, val, col_name):
        if isinstance(val, str) and '%' in val:
            try:
                percent = float(val.split('%')[0])
                time_val, time_unit = col_name.split(' ')
                time_val = int(time_val)

                if (time_unit == 'Min' and (
                   (time_val == 3 and percent > 10) or \
                   (time_val == 5 and percent > 12) or \
                   (time_val == 10 and percent > 15) or \
                   (time_val == 15 and percent > 30) or \
                   (time_val == 30 and percent > 30))) or \
                   (time_unit == 'Hr' and time_val == 3 and percent > 100):
                    return True
            except (ValueError, IndexError):
                pass
        return False

    def _print_tables(self):

        def color_code(val, col_name):
            if self._is_red(val, col_name):
                return colored(val, 'red')
            return val

        print("--- Put OI Data ---")
        for index, row in self.put_oi_data.iterrows():
            for col in self.put_oi_data.columns:
                print(f"{color_code(row[col], col): <20}", end="")
            print()

        print("\n--- Call OI Data ---")
        for index, row in self.call_oi_data.iterrows():
            for col in self.call_oi_data.columns:
                print(f"{color_code(row[col], col): <20}", end="")
            print()

        print("\n--- NIFTY Data ---")
        print(self.nifty_data.to_string())


    def _check_alerts(self):
        red_cell_count = 0

        def count_red_cells(df):
            count = 0
            for col in df.columns:
                if 'Min' in col or 'Hr' in col:
                    for val in df[col]:
                        if self._is_red(val, col):
                            count += 1
            return count

        red_cell_count += count_red_cells(self.put_oi_data)
        red_cell_count += count_red_cells(self.call_oi_data)
        total_cells = (len(self.put_oi_data) * 6) + (len(self.call_oi_data) * 6)

        if total_cells > 0 and (red_cell_count / total_cells) > 0.3:
            try:
                pygame.mixer.music.load("alert.wav") #OR alert.mp3
                pygame.mixer.music.play()
            except pygame.error:
                logger.error("Could not play alert sound. Make sure 'alert.wav' or 'alert.mp3' is in the root directory.")
            logger.warning("ALERT: More than 30% of cells are red!")
