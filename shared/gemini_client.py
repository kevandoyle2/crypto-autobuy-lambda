import requests
import hmac
import hashlib
import time
import base64
import json
import sys

class GeminiClient:
    def __init__(self, public_key, private_key):
        self.base_url = "https://api.gemini.com"
        self.public_key = public_key
        self.private_key = private_key
        self.nonce_counter = int(time.time() * 1000)

    def get_nonce(self):
        """Generate a unique, increasing nonce."""
        self.nonce_counter += 1
        return str(self.nonce_counter)

    def generate_signature(self, payload_base64):
        """Generate HMAC-SHA384 signature for Gemini API."""
        signature = hmac.new(
            self.private_key.encode(),
            payload_base64.encode(),
            hashlib.sha384
        ).hexdigest()
        return signature

    def make_private_post_request(self, endpoint, payload):
        """Make a private POST API request to Gemini."""
        nonce = self.get_nonce()
        payload["request"] = endpoint
        payload["nonce"] = nonce
        payload_json = json.dumps(payload)
        payload_base64 = base64.b64encode(payload_json.encode()).decode()
        timestamp = int(time.time())
        signature = self.generate_signature(payload_base64)

        headers = {
            "X-GEMINI-APIKEY": self.public_key,
            "X-GEMINI-PAYLOAD": payload_base64,
            "X-GEMINI-SIGNATURE": signature,
            "X-GEMINI-TIMESTAMP": str(timestamp),
            "Content-Type": "text/plain",
            "Content-Length": "0",
            "Cache-Control": "no-cache"
        }

        print(f"Requesting {endpoint} with timestamp: {timestamp}, nonce: {nonce}")
        response = requests.post(f"{self.base_url}{endpoint}", headers=headers)
        print(f"Response Status Code: {response.status_code}", flush=True)
        print(f"Response Text: {response.text}", flush=True)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            error_message = f"HTTP Error: {e}, Response: {response.text}"
            print(error_message, flush=True)
            raise Exception(error_message)
        return response.json()

    def make_private_get_request(self, endpoint, params=None):
        """Make a private GET API request to Gemini."""
        nonce = self.get_nonce()
        payload = {
            "request": endpoint,
            "nonce": nonce
        }
        if params:
            payload.update(params)
        payload_json = json.dumps(payload)
        payload_base64 = base64.b64encode(payload_json.encode()).decode()
        timestamp = int(time.time())
        signature = self.generate_signature(payload_base64)

        headers = {
            "X-GEMINI-APIKEY": self.public_key,
            "X-GEMINI-PAYLOAD": payload_base64,
            "X-GEMINI-SIGNATURE": signature,
            "X-GEMINI-TIMESTAMP": str(timestamp),
            "Content-Type": "text/plain",
            "Content-Length": "0",
            "Cache-Control": "no-cache"
        }

        print(f"Requesting {endpoint} with timestamp: {timestamp}, nonce: {nonce}")
        response = requests.get(f"{self.base_url}{endpoint}", headers=headers)
        print(f"Response Status Code: {response.status_code}", flush=True)
        print(f"Response Text: {response.text}", flush=True)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            error_message = f"HTTP Error: {e}, Response: {response.text}"
            print(error_message, flush=True)
            raise Exception(error_message)
        return response.json()

    def get_balance(self):
        """Fetch account balances from Gemini."""
        endpoint = "/v1/balances"
        payload = {}
        return self.make_private_post_request(endpoint, payload)

    def get_ticker(self, symbol):
        """Fetch ticker data for a given symbol (public endpoint)."""
        endpoint = f"/v2/ticker/{symbol}"
        response = requests.get(f"{self.base_url}{endpoint}")
        response.raise_for_status()
        return response.json()

    def place_order(self, order_details):
        """Place an order on Gemini."""
        endpoint = "/v1/order/new"
        return self.make_private_post_request(endpoint, order_details)

    def get_staking_rates(self):
        """Fetch staking rates and provider IDs from Gemini."""
        endpoint = "/v1/staking/rates"
        return self.make_private_get_request(endpoint)

    def stake_assets(self, staking_payload):
        """Stake assets on Gemini."""
        endpoint = "/v1/staking/stake"
        return self.make_private_post_request(endpoint, staking_payload)