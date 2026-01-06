import requests
import hmac
import hashlib
import time
import base64
import json

class GeminiClient:
    def __init__(self, public_key, private_key):
        self.base_url = "https://api.gemini.com"
        self.public_key = public_key
        self.private_key = private_key.encode()
        # Start from current millisecond timestamp
        self.nonce_counter = int(time.time() * 1000)

    def _get_nonce(self):
        """Generate a unique, strictly increasing nonce (millisecond-based counter)."""
        self.nonce_counter += 1
        return str(self.nonce_counter)

    def _generate_payload(self, endpoint, extra_params=None):
        payload = {
            "request": endpoint,
            "nonce": self._get_nonce(),
        }
        if extra_params:
            payload.update(extra_params)
        return payload

    def _private_request(self, endpoint, params=None):
        if params is None:
            params = {}

        payload_dict = self._generate_payload(endpoint, params)
        payload_json = json.dumps(payload_dict)
        payload_base64 = base64.b64encode(payload_json.encode()).decode()

        signature = hmac.new(
            self.private_key,
            payload_base64.encode(),
            hashlib.sha384
        ).hexdigest()

        headers = {
            "X-GEMINI-APIKEY": self.public_key,
            "X-GEMINI-PAYLOAD": payload_base64,
            "X-GEMINI-SIGNATURE": signature,
            "Content-Type": "text/plain",
            "Content-Length": "0",
            "Cache-Control": "no-cache",
        }

        url = f"{self.base_url}{endpoint}"
        response = requests.post(url, headers=headers)

        try:
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            error_msg = f"Gemini API Error {endpoint}: {response.status_code} - {response.text}"
            raise Exception(error_msg) from e

    def get_balance(self):
        return self._private_request("/v1/balances")

    def get_notional_volume(self):
        return self._private_request("/v1/notionalvolume")

    def get_ticker(self, symbol):
        endpoint = f"/v2/ticker/{symbol}"
        response = requests.get(f"{self.base_url}{endpoint}")
        response.raise_for_status()
        return response.json()

    def place_order(self, order_details):
        return self._private_request("/v1/order/new", order_details)