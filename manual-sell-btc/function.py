import json
import os
import boto3
import requests
import hmac
import hashlib
import time
import base64

# Initialize Secrets Manager client
secrets_client = boto3.client('secretsmanager')

def get_api_keys():
    secret_name = "GeminiApiKeys"
    region_name = "us-east-1"
    try:
        get_secret_value_response = secrets_client.get_secret_value(SecretId=secret_name)
        secret = json.loads(get_secret_value_response['SecretString'])
        public_key = secret['GEMINI_PUBLIC_KEY']
        private_key = secret['GEMINI_PRIVATE_KEY']
        return public_key, private_key
    except Exception as e:
        raise ValueError(f"Error retrieving secrets from AWS Secrets Manager: {str(e)}")

def generate_signature(payload_base64, secret_key, timestamp):
    payload_to_sign = str(timestamp) + payload_base64
    signature = hmac.new(
        secret_key.encode(),
        payload_to_sign.encode(),
        hashlib.sha384
    ).hexdigest()
    return signature

def get_btc_balance(pub_key, priv_key):
    base_url = "https://api.gemini.com"
    endpoint = "/v1/balances"
    nonce = str(int(time.time() * 1000))
    payload = {
        "request": endpoint,
        "nonce": nonce
    }
    payload_json = json.dumps(payload)
    payload_base64 = base64.b64encode(payload_json.encode()).decode()
    timestamp = int(time.time())
    signature = generate_signature(payload_base64, priv_key, timestamp)

    headers = {
        "X-GEMINI-APIKEY": pub_key,
        "X-GEMINI-PAYLOAD": payload_base64,
        "X-GEMINI-SIGNATURE": signature,
        "X-GEMINI-TIMESTAMP": str(timestamp),
        "Content-Type": "text/plain"
    }

    response = requests.post(f"{base_url}{endpoint}", data=payload_base64, headers=headers)
    response.raise_for_status()
    balances = response.json()
    for asset in balances:
        if asset['currency'] == 'BTC':
            return float(asset['available'])
    return 0.0  # No BTC balance found

def _sellBitcoin(sell_size_usd, pub_key, priv_key):
    base_url = "https://api.gemini.com"
    
    # Check BTC balance first
    btc_balance = get_btc_balance(pub_key, priv_key)
    print(f"BTC Available Balance: {btc_balance}")

    # Calculate BTC amount needed to sell for the requested USD size later
    # We need current bid price first for that calculation
    response = requests.get(f"{base_url}/v2/ticker/BTCUSD")
    response.raise_for_status()
    ticker = response.json()
    symbol_spot_price = float(ticker['bid'])
    print(f"Bid Price: {symbol_spot_price}")

    tick_size = 8
    quote_currency_price_increment = 2
    symbol = "BTCUSD"

    factor = 1.001
    execution_price = round(symbol_spot_price * factor, quote_currency_price_increment)
    amount = round((sell_size_usd * 0.998) / execution_price, tick_size)

    if btc_balance < amount:
        error_message = f"Insufficient BTC balance to sell: Need {amount} BTC but only have {btc_balance} BTC."
        print(error_message)
        return {"error": error_message}

    order_payload = {
        "request": "/v1/order/new",
        "nonce": str(int(time.time() * 1000)),
        "symbol": symbol,
        "amount": str(amount),
        "price": str(execution_price),
        "side": "sell",
        "type": "exchange limit",
        "options": ["maker-or-cancel"]
    }

    payload_json = json.dumps(order_payload)
    payload_base64 = base64.b64encode(payload_json.encode()).decode()
    timestamp = int(time.time())
    signature = generate_signature(payload_base64, priv_key, timestamp)

    headers = {
        "X-GEMINI-APIKEY": pub_key,
        "X-GEMINI-PAYLOAD": payload_base64,
        "X-GEMINI-SIGNATURE": signature,
        "X-GEMINI-TIMESTAMP": str(timestamp),
        "Content-Type": "text/plain"
    }

    try:
        order_response = requests.post(f"{base_url}/v1/order/new", data=payload_base64, headers=headers)
        order_response.raise_for_status()
        result = order_response.json()
        print(f'Maker Sell: {result}')
        return result
    except requests.exceptions.HTTPError as http_err:
        try:
            error_resp = http_err.response.json()
            error_msg = error_resp.get('reason') or error_resp.get('message') or str(error_resp)
        except Exception:
            error_msg = str(http_err)
        print(f"Order failed: {error_msg}")
        return {"error": error_msg}
    except Exception as e:
        print(f"Unexpected error during order: {str(e)}")
        return {"error": str(e)}

def lambda_handler(event, context):
    try:
        public_key, private_key = get_api_keys()
        result = _sellBitcoin(2.5, public_key, private_key)
        return {
            'statusCode': 200,
            'body': json.dumps(result if isinstance(result, dict) else {'message': 'End of script'})
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }