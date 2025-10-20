import logging
import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from typing import Dict, Any, Optional, List
import requests
import hashlib, pyotp
from dotenv import load_dotenv
from brokers.base import BrokerBase
from kiteconnect import KiteConnect, KiteTicker
import pandas as pd
from threading import Thread

from logger import logger


load_dotenv()


# --- Zerodha Broker ---
class ZerodhaBroker(BrokerBase):
    def __init__(self, without_totp):
        super().__init__()
        self.without_totp = without_totp
        self.kite, self.auth_response_data = self.authenticate()
        # self.kite.set_access_token(self.auth_response_data["access_token"])
        self.kite_ws = KiteTicker(api_key=os.getenv('BROKER_API_KEY'), access_token=self.auth_response_data["access_token"])
        self.tick_counter = 0
        self.symbols = []
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 10
        
    def authenticate(self):
        api_key = os.getenv('BROKER_API_KEY')
        api_secret = os.getenv('BROKER_API_SECRET')

        if not all([api_key, api_secret]):
            raise Exception("BROKER_API_KEY and BROKER_API_SECRET must be set in the .env file.")

        kite = KiteConnect(api_key=api_key)

        print("="*80)
        print("Please follow these steps to authenticate:")
        print("1. Open the following URL in your web browser:")
        print(f"   {kite.login_url()}")
        print("2. Log in to your Zerodha account.")
        print("3. You will be redirected to a new URL. Copy the 'request_token' from that URL.")
        print("   (It looks like: ...&request_token=YOUR_TOKEN&action=...)")
        print("4. Paste the request_token below and press Enter.")
        print("="*80)

        request_token = input("Enter the request_token: ")

        try:
            resp = kite.generate_session(request_token, api_secret)
        except Exception as e:
            logger.error(f"Authentication failed: {e}")
            sys.exit(1)
        
        logger.info("Authentication successful!")
        return kite, resp
    
    def get_orders(self):
        return self.kite.orders()
    
    def get_quote(self, symbol, exchange):
        if ":" not in symbol:   
            symbol = exchange + ":" + symbol
        return self.kite.quote(symbol)
    
    def place_gtt_order(self, symbol, quantity, price, transaction_type, order_type, exchange, product, tag="Unknown"):
        if order_type not in ["LIMIT", "MARKET"]:
            raise ValueError(f"Invalid order type: {order_type}")
        
        if transaction_type not in ["BUY", "SELL"]:
            raise ValueError(f"Invalid transaction type: {transaction_type}")
        
        order_obj = {
            "exchange": exchange,
            "tradingsymbol": symbol,
            "transaction_type": transaction_type,
            "quantity": quantity,
            "order_type": order_type,
            "product": product,
            "price": price,
            "tag": tag
        }
        last_price = self.get_quote(symbol, exchange)[exchange + ":" + symbol]['last_price']
        order_id = self.kite.place_gtt(trigger_type=self.kite.GTT_TYPE_SINGLE, tradingsymbol=symbol, exchange=exchange, trigger_values=[price], last_price=last_price, orders=order_obj)
        return order_id['trigger_id']
    
    def place_order(self, symbol, quantity, price, transaction_type, order_type, variety, exchange, product, tag="Unknown"):
        if order_type == "LIMIT":
            order_type = self.kite.ORDER_TYPE_LIMIT
        elif order_type == "MARKET":
            order_type = self.kite.ORDER_TYPE_MARKET
        else:
            raise ValueError(f"Invalid order type: {order_type}")
        
        if transaction_type == "BUY":
            transaction_type = self.kite.TRANSACTION_TYPE_BUY
        elif transaction_type == "SELL":
            transaction_type = self.kite.TRANSACTION_TYPE_SELL
        else:
            raise ValueError(f"Invalid transaction type: {transaction_type}")
        
        if variety == "REGULAR":
            variety = self.kite.VARIETY_REGULAR
        else:
            raise ValueError(f"Invalid variety: {variety}")
        
        logger.info(f"Placing order for {symbol} with quantity {quantity} at {price} with order type {order_type} and transaction type {transaction_type}, variety {variety}, exchange {exchange}, product {product}, tag {tag}")
        order_attempt = 0
        try:
            while order_attempt < 5:
                order_id = self.kite.place_order(
                    variety=variety,
                    exchange=exchange,
                    tradingsymbol=symbol,
                    transaction_type=transaction_type,
                    quantity=quantity,
                    product=product,
                    order_type=order_type,
                    price=price if order_type == 'LIMIT' else None,
                    tag=tag
                )
                logger.info(f"Order placed: {order_id}")
                return order_id
            logger.error(f"Order placement failed after 5 attempts")
            return -1
        except Exception as e:
            logger.error(f"Order placement failed: {e}")
            return -1
    
    def get_quote(self, symbol):
        return self.kite.quote(symbol)
    

    def get_positions(self):
        return self.kite.positions()

    def historical_data(self, instrument_token, from_date, to_date, interval):
        return self.kite.historical_data(instrument_token, from_date, to_date, interval)

    def symbols_to_subscribe(self, symbols):
        self.symbols = symbols

    ## Websocket Calllbacks
    def on_ticks(self, ws, ticks):  # noqa
        """
        This callback is called when the websocket receives a tick.
        This is the skeleton of the callback.
        The actual implementation has to be handled by the user
        """
        # Callback to receive ticks.
        logger.info("Ticks: {}".format(ticks))
        # self.tick_counter += 1

    def on_connect(self, ws, response):  # noqa
        """
        This callback is called when the websocket is connected.
        This is the skeleton of the callback.
        The actual implementation has to be handled by the user
        """
        # Callback on successful connect.
        # Subscribe to a list of instrument_tokens (RELIANCE and ACC here).
        logger.info("Connected")
        self.reconnect_attempts = 0 # Reset reconnect attempts on successful connection
        # Set RELIANCE to tick in `full` mode.
        ws.subscribe(self.symbols)
        ws.set_mode(ws.MODE_FULL, self.symbols)


    def on_order_update(self, ws, data):
        """
        This callback is called when the websocket receives an order update.
        This is the skeleton of the callback.
        The actual implementation has to be handled by the user
        """
        logger.info("Order update : {}".format(data))

    def on_close(self, ws, code, reason):
        """
        This callback is called when the websocket is closed.
        This is the skeleton of the callback.
        The actual implementation has to be handled by the user
        """
        logger.info("Connection closed: {code} - {reason}".format(code=code, reason=reason))


    # Callback when connection closed with error.
    def on_error(self, ws, code, reason):
        """
        This callback is called when the websocket encounters an error.
        This is the skeleton of the callback.
        The actual implementation has to be handled by the user
        """
        logger.info("Connection error: {code} - {reason}".format(code=code, reason=reason))


    # Callback when reconnect is on progress
    def on_reconnect(self, ws, attempts_count):
        """
        This callback is called when the websocket is reconnecting.
        This is the skeleton of the callback.
        The actual implementation has to be handled by the user
        """
        logger.info("Reconnecting: {}".format(attempts_count))
        self.reconnect_attempts = attempts_count
        if self.reconnect_attempts > self.max_reconnect_attempts:
            logger.error("Too many reconnect attempts. Exiting.")
            sys.exit(1)


    # Callback when all reconnect failed (exhausted max retries)
    def on_noreconnect(self, ws):
        """
        This callback is called when the websocket fails to reconnect.
        This is the skeleton of the callback.
        The actual implementation has to be handled by the user
        """
        logger.error("Reconnect failed after multiple attempts. The market might be closed or there could be a network issue.")
        sys.exit(1)
    
    def download_instruments(self):
        instruments = self.kite.instruments()
        self.instruments_df = pd.DataFrame(instruments)
    
    def get_instruments(self):
        return self.instruments_df
    
    def connect_websocket(self):
        self.kite_ws.on_ticks = self.on_ticks
        self.kite_ws.on_connect = self.on_connect
        self.kite_ws.on_order_update = self.on_order_update
        self.kite_ws.on_close = self.on_close
        self.kite_ws.on_error = self.on_error
        self.kite_ws.on_reconnect = self.on_reconnect
        self.kite_ws.on_noreconnect = self.on_noreconnect
        self.kite_ws.connect(threaded=True)
        
