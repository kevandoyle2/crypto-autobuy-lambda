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

def _buyEthereum(buy_size):
    public_key, private_key = get_api_keys()
    gemini = GeminiClient(public_key, private_key)

    # Check USD balance first
    balances = gemini.get_balance()
    usd_balance = 0.0
    for asset in balances:
        if asset['currency'] == 'USD':
            usd_balance = float(asset['available'])
            break
    print(f"USD Available Balance: {usd_balance}")

    if usd_balance < buy_size:
        error_message = f"Insufficient USD balance to cover buy amount: {buy_size} USD requested but only {usd_balance} available."
        print(error_message)
        return {"error": error_message}

    # Get current ask price
    ticker = gemini.get_ticker("ETHUSD")
    symbol_spot_price = float(ticker['ask'])
    print(f"Spot Ask Price: {symbol_spot_price}")

    tick_size = 6
    quote_currency_price_increment = 2
    symbol = "ETHUSD"
    
    factor = 0.998  # slippage factor
    execution_price = str(round(symbol_spot_price * factor, quote_currency_price_increment))
    eth_amount = round((buy_size * 0.998) / float(execution_price), tick_size)

    order_payload = {
        "symbol": symbol,
        "amount": str(eth_amount),
        "price": execution_price,
        "side": "buy",
        "type": "exchange limit",
        "options": ["maker-or-cancel"]
    }

    try:
        result = gemini.place_order(order_payload)
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
        result = _buyEthereum(50)
        return {
            'statusCode': 200,
            'body': json.dumps(result if isinstance(result, dict) else {'message': 'End of script'})
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }