import json
import boto3
import requests
import hmac
import hashlib
import time
import base64

secrets_client = boto3.client('secretsmanager')

def get_api_keys():
    secret_name = "GeminiApiKeys"
    try:
        get_secret_value_response = secrets_client.get_secret_value(SecretId=secret_name)
        secret = json.loads(get_secret_value_response['SecretString'])
        return secret['GEMINI_PUBLIC_KEY'], secret['GEMINI_PRIVATE_KEY']
    except Exception as e:
        raise ValueError(f"Error retrieving secrets from AWS Secrets Manager: {str(e)}")

def generate_signature(payload_base64, secret_key, timestamp):
    payload_to_sign = str(timestamp) + payload_base64
    return hmac.new(secret_key.encode(), payload_to_sign.encode(), hashlib.sha384).hexdigest()

def _convertGUSDtoUSD(pub_key, priv_key):
    base_url = "https://api.gemini.com"

    # Step 1: Get balances (authenticated request)
    endpoint = "/v1/balances"
    nonce = str(int(time.time() * 1000))
    payload_dict = {
        "request": endpoint,
        "nonce": nonce
    }
    payload_json = json.dumps(payload_dict)
    payload_base64 = base64.b64encode(payload_json.encode()).decode()
    timestamp = int(time.time())

    headers = {
        "X-GEMINI-APIKEY": pub_key,
        "X-GEMINI-PAYLOAD": payload_base64,
        "X-GEMINI-SIGNATURE": generate_signature(payload_base64, priv_key, timestamp),
        "X-GEMINI-TIMESTAMP": str(timestamp),
        "Content-Type": "text/plain"
    }

    response = requests.post(f"{base_url}{endpoint}", data=payload_base64, headers=headers)
    response.raise_for_status()
    balances = response.json()

    gusd_balance = 0
    for balance in balances:
        if balance.get('currency') == 'GUSD':
            gusd_balance = float(balance.get('amount', 0))
            break

    if gusd_balance <= 0:
        return {"message": "No GUSD balance available to convert."}

    # Step 2: Place limit sell order for GUSD at $1.00
    endpoint = "/v1/order/new"
    nonce = str(int(time.time() * 1000))  # new nonce for order request
    order_payload_dict = {
        "request": endpoint,
        "nonce": nonce,
        "symbol": "GUSDUSD",
        "amount": str(gusd_balance),
        "price": "1.00",
        "side": "sell",
        "type": "exchange limit",
        "options": ["maker-or-cancel"]
    }
    order_payload_json = json.dumps(order_payload_dict)
    order_payload_base64 = base64.b64encode(order_payload_json.encode()).decode()
    timestamp = int(time.time())

    headers = {
        "X-GEMINI-APIKEY": pub_key,
        "X-GEMINI-PAYLOAD": order_payload_base64,
        "X-GEMINI-SIGNATURE": generate_signature(order_payload_base64, priv_key, timestamp),
        "X-GEMINI-TIMESTAMP": str(timestamp),
        "Content-Type": "text/plain"
    }

    order_response = requests.post(f"{base_url}{endpoint}", data=order_payload_base64, headers=headers)
    order_response.raise_for_status()
    result = order_response.json()
    print(result)
    return result


def lambda_handler(event, context):
    try:
        public_key, private_key = get_api_keys()
        result = _convertGUSDtoUSD(public_key, private_key)
        return {
            'statusCode': 200,
            'body': json.dumps(result)
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }