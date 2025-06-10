import json
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

    # Check GUSD balance
    balances = gemini.get_balance()
    gusd_balance = 0.0
    for asset in balances:
        if asset['currency'] == 'GUSD':
            gusd_balance = float(asset['available'])
            break
    
    print(f"GUSD Available Balance: ${gusd_balance}")

    # Estimate fees (0.01% taker fee for stablecoin pairs)
    fee_rate = 0.0001  # 0.01% for ETHGUSD
    required_funds = buy_size * (1 + fee_rate)
    print(f"Required funds (including {fee_rate*100}% fee): ${required_funds:.2f}")

    if gusd_balance < required_funds:
        error_message = f"Insufficient GUSD balance: ${gusd_balance} available, need ${required_funds:.2f}. Fund your GUSD account."
        print(error_message)
        return {"error": error_message}

    # Get current ask price
    ticker = gemini.get_ticker("ETHGUSD")
    symbol_spot_price = float(ticker['ask'])
    print(f"Spot Ask Price: ${symbol_spot_price} GUSD")

    tick_size = 6
    quote_currency_price_increment = 2
    symbol = "ETHGUSD"
    min_quantity = 0.00001  # Gemini's minimum ETH order size

    factor = 0.998  # Slippage factor
    execution_price = str(round(symbol_spot_price * factor, quote_currency_price_increment))
    eth_amount = round((buy_size * 0.998) / float(execution_price), tick_size)
    
    if eth_amount < min_quantity:
        error_message = f"Calculated ETH amount ({eth_amount} ETH) is below minimum order size ({min_quantity} ETH)."
        print(error_message)
        return {"error": error_message}

    order_cost = float(execution_price) * eth_amount
    order_fee = order_cost * fee_rate
    total_order_cost = order_cost + order_fee
    print(f"Order: {eth_amount} ETH at ${execution_price} GUSD = ${order_cost:.2f}")
    print(f"Estimated fee: ${order_fee:.2f}")
    print(f"Total order cost: ${total_order_cost:.2f} GUSD")

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
        result = _buyEthereum(27.2)
        return {
            'statusCode': 200,
            'body': json.dumps(result if isinstance(result, dict) else {'message': 'End of script'})
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }