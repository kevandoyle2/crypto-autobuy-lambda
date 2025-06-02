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

# This function converts all your GUSD to USD
def _convertGUSDtoUSD(pub_key, priv_key):
    base_url = "https://api.gemini.com"
    endpoint = "/v1/balances"
    timestamp = int(time.time())

    # Get account balances
    headers = {
        "X-GEMINI-APIKEY": pub_key,
        "X-GEMINI-SIGNATURE": generate_signature("", priv_key, timestamp),
        "X-GEMINI-TIMESTAMP": str(timestamp)
    }
    response = requests.get(f"{base_url}{endpoint}", headers=headers)
    response.raise_for_status()
    balances = response.json()

    gusd_balance = 0
    for balance in balances:
        if balance.get('currency') == 'GUSD':
            gusd_balance = float(balance.get('amount', 0))
            break

    if gusd_balance > 0:
        # Convert GUSD to USD via a market sell order
        endpoint = "/v1/order/new"
        payload = json.dumps({
            "request": "/v1/order/new",
            "nonce": str(int(time.time() * 1000)),
            "symbol": "GUSDUSD",
            "amount": str(gusd_balance),
            "price": "1.00",  # Market order, price is optional
            "side": "sell",
            "type": "exchange limit",
            "options": ["maker-or-cancel"]
        })
        headers = {
            "X-GEMINI-APIKEY": pub_key,
            "X-GEMINI-SIGNATURE": generate_signature(payload, priv_key, timestamp),
            "X-GEMINI-TIMESTAMP": str(timestamp),
            "Content-Type": "text/plain"
        }
        order_response = requests.post(f"{base_url}{endpoint}", data=payload, headers=headers)
        order_response.raise_for_status()
        print(order_response.json())
    else:
        return "there is no GUSD to convert in your account"

def lambda_handler(event, context):
    try:
        # Retrieve API keys from Secrets Manager
        public_key, private_key = get_api_keys()
        
        result = _convertGUSDtoUSD(public_key, private_key)
        if result:
            return {
                'statusCode': 200,
                'body': json.dumps(result)
            }
        return {
            'statusCode': 200,
            'body': json.dumps('End of script')
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }