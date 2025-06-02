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

def get_usd_balance(pub_key, priv_key):
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
        if asset['currency'] == 'USD':
            return float(asset['available'])
    return 0.0  # No USD balance found

def _buyEthereum(buy_size, pub_key, priv_key):
    base_url = "https://api.gemini.com"
    
    # Check USD balance before buying
    usd_balance = get_usd_balance(pub_key, priv_key)
    print(f"USD Available Balance: {usd_balance}")

    if usd_balance < buy_size:
        error_message = f"Insufficient USD balance to cover buy amount: {buy_size} USD requested but only {usd_balance} available."
        print(error_message)
        return {"error": error_message}

    # Get current ask price
    response = requests.get(f"{base_url}/v2/ticker/ETHUSD")
    response.raise_for_status()
    ticker = response.json()
    symbol_spot_price = float(ticker['ask'])
    print(f"Spot Ask Price: {symbol_spot_price}")

    tick_size = 6
    quote_currency_price_increment = 2
    symbol = "ETHUSD"
    
    factor = 0.998  # slippage factor
    execution_price = str(round(symbol_spot_price * factor, quote_currency_price_increment))
    eth_amount = round((buy_size * factor) / float(execution_price), tick_size)

    order_payload = {
        "request": "/v1/order/new",
        "nonce": str(int(time.time() * 1000)),
        "symbol": symbol,
        "amount": str(eth_amount),
        "price": execution_price,
        "side": "buy",
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
        print(f'Maker Buy: {result}')
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
        result = _buyEthereum(27.2, public_key, private_key)
        return {
            'statusCode': 200,
            'body': json.dumps(result if isinstance(result, dict) else {'message': 'End of script'})
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }