import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from brokers.base import BrokerBase
from neo_api_client import NeoAPI
from logger import logger
import pandas as pd
import pyotp

class KotakNeoBroker(BrokerBase):
    def __init__(self):
        super().__init__()
        self.client = None
        self.authenticate()

    def authenticate(self):
        consumer_key = os.getenv("KOTAK_CONSUMER_KEY")
        consumer_secret = os.getenv("KOTAK_CONSUMER_SECRET")
        mobile_number = os.getenv("KOTAK_MOBILE_NUMBER")
        password = os.getenv("KOTAK_PASSWORD")
        totp_secret = os.getenv("KOTAK_TOTP_SECRET")

        if not all([consumer_key, consumer_secret, mobile_number, password, totp_secret]):
            raise Exception("Missing one or more required Kotak Neo environment variables.")

        self.client = NeoAPI(consumer_key=consumer_key, consumer_secret=consumer_secret, environment='uat')

        # Perform the login using mobile number and password
        self.client.login(mobilenumber=mobile_number, password=password)

        # Generate the TOTP
        totp = pyotp.TOTP(totp_secret).now()

        # Complete the 2FA using the generated TOTP
        self.client.session_2fa(OTP=totp)

        logger.info("Kotak Neo authenticated successfully.")

    def download_instruments(self):
        try:
            self.instruments_df = pd.DataFrame(self.client.scrip_master())
            logger.info("Kotak Neo instruments downloaded successfully.")
        except Exception as e:
            logger.error(f"Error downloading Kotak Neo instruments: {e}")

    def get_quote(self, symbol, exchange_segment):
        instrument_token = self.instruments_df[self.instruments_df['symbol'] == symbol].iloc[0]['instrument_token']
        instrument_tokens = [{"instrument_token": str(instrument_token), "exchange_segment": exchange_segment}]
        return self.client.quotes(instrument_tokens=instrument_tokens, quote_type="ltp")

    def historical_data(self, instrument_token, from_date, to_date, interval):
        # The Kotak Neo API client does not have a historical data method.
        # This needs to be implemented by calling the historical API endpoint directly.
        # For now, we will return an empty list.
        return []
