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

def _sellEthereum(sell_size_usd):
    public_key, private_key = get_api_keys()
    gemini = GeminiClient(public_key, private_key)

    # Check ETH balance first
    try:
        balances = gemini.get_balance()
    except Exception as e:
        error_message = f"Failed to retrieve balances: {str(e)}"
        print(error_message)
        return {"error": error_message}

    eth_balance = 0.0
    for asset in balances:
        if asset['currency'] == 'ETH':
            eth_balance = float(asset['available'])
            break
    print(f"ETH Available Balance: {eth_balance}")

    # Get current bid price
    try:
        ticker = gemini.get_ticker("ETHUSD")
        symbol_spot_price = float(ticker['bid'])
        print(f"Bid Price: {symbol_spot_price}")
    except Exception as e:
        error_message = f"Failed to retrieve ticker data: {str(e)}"
        print(error_message)
        return {"error": error_message}

    tick_size = 6
    quote_currency_price_increment = 2
    symbol = "ETHUSD"
    
    factor = 1.001
    execution_price = str(round(symbol_spot_price * factor, quote_currency_price_increment))
    eth_amount = round((sell_size_usd * 0.998) / float(execution_price), tick_size)
    print(f"Calculated execution price: {execution_price}, amount to sell: {eth_amount} ETH")

    if eth_balance < eth_amount:
        error_message = f"Insufficient ETH balance to sell: Need {eth_amount} ETH but only have {eth_balance} ETH."
        print(error_message)
        return {"error": error_message}

    order_payload = {
        "symbol": symbol,
        "amount": str(eth_amount),
        "price": execution_price,
        "side": "sell",
        "type": "exchange limit",
        "options": ["maker-or-cancel"]
    }

    try:
        result = gemini.place_order(order_payload)
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
        result = _sellEthereum(50)
        return {
            'statusCode': 200,
            'body': json.dumps(result if isinstance(result, dict) else {'message': 'End of script'})
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }