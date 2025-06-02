import json
import os
import boto3
from shared.gemini_client import GeminiClient

# Initialize SSM client
ssm_client = boto3.client('ssm')

def get_api_keys():
    try:
        response = ssm_client.get_parameter(
            Name='GeminiApiKeys',
            WithDecryption=True
        )
        secret = json.loads(response['Parameter']['Value'])
        public_key = secret['API key']
        private_key = secret['API Secret']
        return public_key, private_key
    except Exception as e:
        raise ValueError(f"Error retrieving API keys from AWS SSM Parameter Store: {str(e)}")

def _convertGUSDtoUSD():
    public_key, private_key = get_api_keys()
    gemini = GeminiClient(public_key, private_key)

    # Step 1: Get balances
    balances = gemini.get_balance()
    gusd_balance = 0
    for balance in balances:
        if balance.get('currency') == 'GUSD':
            gusd_balance = float(balance.get('amount', 0))
            break

    if gusd_balance <= 0:
        return {"message": "there is no GUSD to convert in your account"}

    # Step 2: Place limit sell order for GUSD at $1.00
    order_payload = {
        "symbol": "GUSDUSD",
        "amount": str(gusd_balance),
        "price": "1.00",
        "side": "sell",
        "type": "exchange limit",
        "options": ["maker-or-cancel"]
    }

    try:
        result = gemini.place_order(order_payload)
        print(f"Order Result: {result}")
        return result
    except Exception as e:
        print(f"Error placing order: {str(e)}")
        return {"error": str(e)}

def lambda_handler(event, context):
    try:
        result = _convertGUSDtoUSD()
        return {
            'statusCode': 200,
            'body': json.dumps(result)
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }