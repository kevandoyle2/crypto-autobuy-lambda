import json
import time
import hmac
import base64
import hashlib
from typing import Any, Dict, Optional, Tuple

import requests


class GeminiClient:

    def __init__(self, public_key: str, private_key: str, timeout: Tuple[int, int] = (5, 20)):
        self.base_url = "https://api.gemini.com"
        self.public_key = public_key
        self.private_key = private_key.encode()

        # Strictly increasing nonce (ms-based counter)
        self.nonce_counter = int(time.time() * 1000)

        # requests timeout: (connect_timeout, read_timeout)
        self.timeout = timeout

        # Reuse connections for performance/reliability
        self.session = requests.Session()

    def _get_nonce(self) -> str:
        self.nonce_counter += 1
        return str(self.nonce_counter)

    def _generate_payload(self, endpoint: str, extra_params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "request": endpoint,
            "nonce": self._get_nonce(),
        }
        if extra_params:
            payload.update(extra_params)
        return payload

    def _private_request(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Any:
        if params is None:
            params = {}

        payload_dict = self._generate_payload(endpoint, params)
        payload_json = json.dumps(payload_dict)
        payload_base64 = base64.b64encode(payload_json.encode()).decode()

        signature = hmac.new(
            self.private_key,
            payload_base64.encode(),
            hashlib.sha384,
        ).hexdigest()

        headers = {
            "X-GEMINI-APIKEY": self.public_key,
            "X-GEMINI-PAYLOAD": payload_base64,
            "X-GEMINI-SIGNATURE": signature,
            "Content-Type": "text/plain",
            "Cache-Control": "no-cache",
        }

        url = f"{self.base_url}{endpoint}"
        response = self.session.post(url, headers=headers, timeout=self.timeout)

        # IMPORTANT: preserve HTTPError type + response for upstream handlers
        response.raise_for_status()

        try:
            return response.json()
        except ValueError:
            raise ValueError(f"Non-JSON response from Gemini {endpoint}: {response.text}")

    def _public_get(self, endpoint: str) -> Any:
        url = f"{self.base_url}{endpoint}"
        response = self.session.get(url, timeout=self.timeout)
        response.raise_for_status()
        try:
            return response.json()
        except ValueError:
            raise ValueError(f"Non-JSON response from Gemini {endpoint}: {response.text}")

    # ---------- Public API (compatible with your existing code) ----------

    def get_balance(self):
        return self._private_request("/v1/balances")

    def get_notional_volume(self):
        return self._private_request("/v1/notionalvolume")

    def get_ticker(self, symbol: str):
        return self._public_get(f"/v2/ticker/{symbol}")

    def place_order(self, order_details: Dict[str, Any]):
        return self._private_request("/v1/order/new", order_details)

    # ---------- Optional helpers ----------

    def get_order_status(self, order_id: str):
        return self._private_request("/v1/order/status", {"order_id": str(order_id)})

    def cancel_order(self, order_id: str):
        return self._private_request("/v1/order/cancel", {"order_id": str(order_id)})