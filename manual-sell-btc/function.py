import json
import os
import boto3
import gemini

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

symbol = "BTCUSD"
tick_size = 8
quote_currency_price_increment = 2

def _sellBitcoin(sell_size_usd, pub_key, priv_key):
    trader = gemini.PrivateClient(pub_key, priv_key)

    # Get the current bid price (what buyers are offering)
    symbol_spot_price = float(trader.get_ticker(symbol)['bid'])
    print(f"Bid Price: {symbol_spot_price}")

    factor = 1.001  # Increase price slightly to try to sell above spot (or set to 1.0 to match spot)
    execution_price = str(round(symbol_spot_price * factor, quote_currency_price_increment))

    # Calculate how much BTC to sell for the given USD size (minus fees)
    amount = round((sell_size_usd * 0.998) / float(execution_price), tick_size)

    # Place the sell order
    sell = trader.new_order(symbol, str(amount), execution_price, "sell", ["maker-or-cancel"])
    print(f'Maker Sell: {sell}')
    return sell

def lambda_handler(event, context):
    try:
        public_key, private_key = get_api_keys()

        # Specify how much USD worth of BTC to sell
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
