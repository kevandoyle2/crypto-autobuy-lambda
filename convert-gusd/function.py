import json
import boto3
import time
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

    # Step 2: Check market status
    try:
        symbol_details = gemini.get_symbol_details("gusdusd")
        print(f"Symbol Details: {symbol_details}")
        if not symbol_details.get("is_trading_enabled", True):  # Adjust based on actual response
            return {"error": "Market for gusdusd is not open"}
    except Exception as e:
        print(f"Error checking market status: {str(e)}")
        return {"error": f"Failed to check market status: {str(e)}"}

    # Step 3: Place limit sell order for GUSD at $1.00
    order_payload = {
        "symbol": "gusdusd",
        "amount": f"{gusd_balance:.8f}",
        "price": "1.0000",
        "side": "sell",
        "type": "exchange limit",
        "options": ["immediate-or-cancel"]
    }

    try:
        result = gemini.place_order(order_payload)
        print(f"Order Result: {result}")
        return result
    except Exception as e:
        error_message = f"Error placing order: {str(e)}"
        print(error_message)
        return {"error": error_message}

def lambda_handler(event, context):
    try:
        result = _convertGUSDtoUSD()
        return {
            'statusCode': 200,
            'body': json.dumps(result)
        }
    except Exception as e:
        error_message = f"Lambda execution error: {str(e)}"
        print(error_message)
        return {
            'statusCode': 500,
            'body': json.dumps({'error': error_message})
        }