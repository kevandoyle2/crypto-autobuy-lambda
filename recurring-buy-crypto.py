import json
import logging
import requests
import boto3
import os
from shared.gemini_client import GeminiClient

# Configuration
TOTAL_DEPOSIT = 170
TOTAL_ORDER = TOTAL_DEPOSIT / 2
BTC_PERCENTAGE = 66
ETH_PERCENTAGE = 34

# Calculate amounts
BTC_AMOUNT = round(TOTAL_ORDER * (BTC_PERCENTAGE / 100.0), 2)
ETH_AMOUNT = round(TOTAL_ORDER * (ETH_PERCENTAGE / 100.0), 2)

BUY_CONFIG = {
    "BTC": {
        "amount": BTC_AMOUNT,
        "symbol": "BTCGUSD",
        "tick_size": 8,
        "min_quantity": 0.0001,
        "slippage_factor": 0.999
    },
    "ETH": {
        "amount": ETH_AMOUNT,
        "symbol": "ETHGUSD",
        "tick_size": 6,
        "min_quantity": 0.00001,
        "slippage_factor": 0.998
    }
}

# Initialize clients
ssm_client = boto3.client('ssm')
sns_client = boto3.client('sns')
SNS_TOPIC_ARN = os.environ.get('SNS_TOPIC_ARN')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def send_alert(subject, message):
    """Send an SNS alert if configured."""
    if not SNS_TOPIC_ARN:
        logger.warning("SNS topic ARN not set, skipping alert.")
        return
    try:
        sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=subject,
            Message=message
        )
        logger.info(f"Alert sent: {subject}")
    except Exception as e:
        logger.error(f"Failed to send SNS alert: {str(e)}")

def get_api_keys():
    try:
        response = ssm_client.get_parameter(Name='GeminiApiKeys', WithDecryption=True)
        secret = json.loads(response['Parameter']['Value'])
        return secret['API key'], secret['API Secret']
    except Exception as e:
        raise ValueError(f"Error retrieving API keys from AWS SSM Parameter Store: {str(e)}")

def buy_crypto(asset, buy_size, symbol, tick_size, min_quantity, slippage_factor):
    public_key, private_key = get_api_keys()
    gemini = GeminiClient(public_key, private_key)

    # Check GUSD balance
    try:
        balances = gemini.get_balance()
        gusd_balance = 0.0
        for asset_info in balances:
            if asset_info['currency'] == 'GUSD':
                gusd_balance = float(asset_info['available'])
                break
        logger.info(f"GUSD Available Balance: ${gusd_balance}")
    except requests.exceptions.HTTPError as http_err:
        try:
            error_resp = http_err.response.json()
            error_msg = error_resp.get('reason') or error_resp.get('message') or str(error_resp)
            if error_resp.get('reason') == "ApiKeyIpFilteringFailure":
                error_message = f"API key blocked due to IP filtering for {asset}. Update Gemini IP allowlist or disable IP restrictions."
                logger.error(error_message)
                send_alert("Crypto Buy Failed - IP Filtering", error_message)
                return {"error": error_message}
        except Exception:
            error_msg = str(http_err)
        logger.error(f"Balance check failed for {asset}: {error_msg}")
        send_alert("Crypto Buy Failed - Balance Check", error_msg)
        return {"error": error_msg}

    # Estimate fees
    fee_rate = 0.0001
    required_funds = buy_size * (1 + fee_rate)
    logger.info(f"Required funds for {asset} (including {fee_rate*100}% fee): ${required_funds:.2f}")

    if gusd_balance < required_funds:
        error_message = f"Insufficient GUSD balance for {asset}: ${gusd_balance} available, need ${required_funds:.2f}. Fund your GUSD account."
        logger.error(error_message)
        send_alert("Crypto Buy Failed - Insufficient Funds", error_message)
        return {"error": error_message}

    # Get current ask price
    try:
        ticker = gemini.get_ticker(symbol)
        symbol_spot_price = float(ticker['ask'])
        logger.info(f"Spot Ask Price for {symbol}: ${symbol_spot_price} GUSD")
    except Exception as e:
        error_message = f"Failed to get ticker for {symbol}: {str(e)}"
        logger.error(error_message)
        send_alert("Crypto Buy Failed - Ticker", error_message)
        return {"error": error_message}

    quote_currency_price_increment = 2
    execution_price = str(round(symbol_spot_price * slippage_factor, quote_currency_price_increment))
    crypto_amount = round((buy_size * 0.998) / float(execution_price), tick_size)

    if crypto_amount < min_quantity:
        error_message = f"Calculated {asset} amount ({crypto_amount} {asset}) is below minimum order size ({min_quantity} {asset})."
        logger.error(error_message)
        send_alert("Crypto Buy Failed - Order Too Small", error_message)
        return {"error": error_message}

    order_cost = float(execution_price) * crypto_amount
    order_fee = order_cost * fee_rate
    total_order_cost = order_cost + order_fee
    logger.info(f"Order: {crypto_amount} {asset} at ${execution_price} GUSD = ${order_cost:.2f}")
    logger.info(f"Estimated fee: ${order_fee:.2f}")
    logger.info(f"Total order cost: ${total_order_cost:.2f} GUSD")

    order_payload = {
        "symbol": symbol,
        "amount": str(crypto_amount),
        "price": execution_price,
        "side": "buy",
        "type": "exchange limit",
        "options": ["maker-or-cancel"]
    }

    try:
        result = gemini.place_order(order_payload)
        logger.info(f"Maker Buy for {asset}: {result}")
        return result
    except requests.exceptions.HTTPError as http_err:
        try:
            error_resp = http_err.response.json()
            error_msg = error_resp.get('reason') or error_resp.get('message') or str(error_resp)
        except Exception:
            error_msg = str(http_err)
        logger.error(f"Order failed for {asset}: {error_msg}")
        send_alert(f"Crypto Buy Failed - {asset} Order Error", error_msg)
        return {"error": error_msg}
    except Exception as e:
        logger.error(f"Unexpected error for {asset}: {str(e)}")
        send_alert(f"Crypto Buy Failed - {asset} Unexpected", str(e))
        return {"error": str(e)}

def lambda_handler(event, context):
    try:
        results = {}
        total_required_funds = sum(config['amount'] * (1 + 0.0001) for config in BUY_CONFIG.values())
        public_key, private_key = get_api_keys()
        gemini = GeminiClient(public_key, private_key)

        # Check total GUSD balance
        try:
            balances = gemini.get_balance()
            gusd_balance = 0.0
            for asset_info in balances:
                if asset_info['currency'] == 'GUSD':
                    gusd_balance = float(asset_info['available'])
                    break
            logger.info(f"GUSD Available Balance: ${gusd_balance}")
            if gusd_balance < total_required_funds:
                error_message = f"Insufficient GUSD balance for all purchases: ${gusd_balance} available, need ${total_required_funds:.2f}. Fund your GUSD account."
                logger.error(error_message)
                send_alert("Crypto Buy Failed - Insufficient Total Funds", error_message)
                return {'statusCode': 400, 'body': json.dumps({'error': error_message})}
        except requests.exceptions.HTTPError as http_err:
            try:
                error_resp = http_err.response.json()
                error_msg = error_resp.get('reason') or error_resp.get('message') or str(error_resp)
                if error_resp.get('reason') == "ApiKeyIpFilteringFailure":
                    error_message = "API key blocked due to IP filtering. Update Gemini IP allowlist or disable IP restrictions."
                    logger.error(error_message)
                    send_alert("Crypto Buy Failed - IP Filtering", error_message)
                    return {'statusCode': 400, 'body': json.dumps({'error': error_message})}
            except Exception:
                error_msg = str(http_err)
            logger.error(f"Balance check failed: {error_msg}")
            send_alert("Crypto Buy Failed - Balance Check", error_msg)
            return {'statusCode': 400, 'body': json.dumps({'error': error_msg})}

        # Process each asset
        for asset, config in BUY_CONFIG.items():
            result = buy_crypto(
                asset=asset,
                buy_size=config['amount'],
                symbol=config['symbol'],
                tick_size=config['tick_size'],
                min_quantity=config['min_quantity'],
                slippage_factor=config['slippage_factor']
            )
            results[asset] = result

        if any('error' in r for r in results.values()):
            send_alert("Crypto Buy Completed With Errors", json.dumps(results, indent=2))

        return {'statusCode': 200, 'body': json.dumps(results)}

    except Exception as e:
        logger.error(f"Lambda execution failed: {str(e)}")
        send_alert("Crypto Buy Lambda Failed", f"Unhandled Error: {str(e)}")
        return {'statusCode': 500, 'body': json.dumps({'error': str(e)})}