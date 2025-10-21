import os
import requests
import pyotp
import json
import logging
from urllib.parse import urlparse, parse_qs
from kiteconnect import KiteConnect
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("auto_login")

# Load .env variables (from nearest .env when run from any folder)
load_dotenv()

# Get credentials from environment
USER_ID = os.getenv("BROKER_USER_ID")
PASSWORD = os.getenv("BROKER_PASSWORD")
TOTP_SECRET = os.getenv("BROKER_TOTP_SECRET")
API_KEY = os.getenv("BROKER_API_KEY")
API_SECRET = os.getenv("BROKER_API_SECRET")

if not all([USER_ID, PASSWORD, TOTP_SECRET, API_KEY, API_SECRET]):
    raise RuntimeError("One or more required environment variables are missing. Please check .env file.")

# Initialize KiteConnect and a persistent HTTP session
kite = KiteConnect(api_key=API_KEY)
session = requests.Session()

def run_auto_login():
    """Perform headless login with TOTP and save access_token to kite_token.json"""
    try:
        logger.info("Opening login URL to initialize session...")
        session.get(kite.login_url())

        logger.info("Submitting user ID and password...")
        login_payload = {"user_id": USER_ID, "password": PASSWORD}
        login_resp = session.post("https://kite.zerodha.com/api/login", data=login_payload)
        login_data = login_resp.json()
        if login_data.get("status") != "success":
            raise RuntimeError(f"Login failed: {login_data.get('message', 'Unknown error')}")

        request_id = login_data["data"]["request_id"]

        logger.info("Generating TOTP code and submitting 2FA...")
        otp = pyotp.TOTP(TOTP_SECRET).now()
        twofa_payload = {
            "user_id": USER_ID,
            "request_id": request_id,
            "twofa_value": otp,
            "twofa_type": "totp",
            "skip_session": True
        }
        twofa_resp = session.post("https://kite.zerodha.com/api/twofa", data=twofa_payload)
        twofa_data = twofa_resp.json()
        if twofa_data.get("status") != "success":
            raise RuntimeError(f"2FA failed: {twofa_data.get('message', 'Unknown error')}")

        logger.info("Fetching redirect URL to extract request_token...")
        redirect_resp = session.get(kite.login_url() + "&skip_session=true", allow_redirects=True)
        final_url = redirect_resp.url
        parsed_url = urlparse(final_url)
        request_token = parse_qs(parsed_url.query).get("request_token", [None])[0]

        if not request_token:
            raise RuntimeError("Failed to retrieve request_token from redirect URL.")

        logger.info(f"Retrieved request_token: {request_token}")

        logger.info("Exchanging request_token for access_token...")
        user_data = kite.generate_session(request_token, api_secret=API_SECRET)
        access_token = user_data["access_token"]

        logger.info("Saving access_token to kite_token.json...")
        with open("kite_token.json", "w", encoding="utf-8") as f:
            json.dump({"access_token": access_token}, f, indent=2)

        logger.info("Login successful. Access token saved to kite_token.json")

    except Exception as e:
        logger.error(f"Login failed: {e}")
        raise

if __name__ == "__main__":
    run_auto_login()
