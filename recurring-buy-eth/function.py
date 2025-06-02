import json
import os
import boto3
import requests
import hmac
import hashlib
import time

# Initialize Secrets Manager client
secrets_client = boto3.client('secretsmanager')

# Retrieve API keys from Secrets Manager
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

# Generate HMAC-SHA384 signature for Gemini API
def generate_signature(payload, secret_key, timestamp):
    payload_to_sign = str(timestamp) + payload
    signature = hmac.new(
        secret_key.encode(),
        payload_to_sign.encode(),
        hashlib.sha384
    ).hexdigest()
    return signature

symbol = "ETHUSD"
tick_size = 6
quote_currency_price_increment = 2

def _buyEthereum(buy_size, pub_key, priv_key):
    base_url = "https://api.gemini.com"
    
    # Get current ask price using public V2 endpoint
    response = requests.get(f"{base_url}/v2/ticker/ETHUSD")
    response.raise_for_status()
    ticker = response.json()
    symbol_spot_price = float(ticker['ask'])
    print(f"Spot Ask Price: {symbol_spot_price}")

    tick_size = 6
    quote_currency_price_increment = 2
    symbol = "ETHUSD"
    
    factor = 0.998  # Adjusted slippage factor
    execution_price = str(round(symbol_spot_price * factor, quote_currency_price_increment))
    eth_amount = round((buy_size * factor) / float(execution_price), tick_size)

    # Place buy order
    endpoint = "/v1/order/new"
    payload = json.dumps({
        "request": "/v1/order/new",
        "nonce": str(int(time.time() * 1000)),
        "symbol": symbol,
        "amount": str(eth_amount),
        "price": execution_price,
        "side": "buy",
        "type": "exchange limit",
        "options": ["maker-or-cancel"]
    })

    timestamp = int(time.time())
    headers = {
        "X-GEMINI-APIKEY": pub_key,
        "X-GEMINI-SIGNATURE": generate_signature(payload, priv_key, timestamp),
        "X-GEMINI-TIMESTAMP": str(timestamp),
        "Content-Type": "text/plain"
    }

    order_response = requests.post(f"{base_url}{endpoint}", data=payload, headers=headers)
    order_response.raise_for_status()
    result = order_response.json()
    print(f'Maker Buy: {result}')
    return result

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