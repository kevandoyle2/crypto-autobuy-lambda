import json
import logging
import requests
import boto3
import os
from decimal import Decimal, ROUND_DOWN
from shared.gemini_client import GeminiClient

# ----------------------------
# Configuration
# ----------------------------

# Fee model used everywhere (funds check, sizing, logs)
FEE_RATE = Decimal("0.0001")  # 0.01%

TOTAL_DEPOSIT = Decimal("170")

# Maximum amount we want to spend per Lambda execution (half of TOTAL_DEPOSIT)
MAX_BUY = (TOTAL_DEPOSIT / 2).quantize(Decimal("0.01"))

# Percentage allocations
BTC_PERCENTAGE = Decimal("66")
ETH_PERCENTAGE = Decimal("34")

# Net budget so that: net_total * (1 + fee_rate) <= MAX_BUY
TOTAL_NET = (MAX_BUY / (Decimal("1") + FEE_RATE)).quantize(Decimal("0.01"), rounding=ROUND_DOWN)

# Split net across assets in cents, with remainder assigned to ETH so totals match exactly
BTC_AMOUNT = (TOTAL_NET * (BTC_PERCENTAGE / Decimal("100"))).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
ETH_AMOUNT = (TOTAL_NET - BTC_AMOUNT).quantize(Decimal("0.01"))

BUY_CONFIG = {
    "BTC": {
        "amount": BTC_AMOUNT,
        "symbol": "BTCGUSD",
        "tick_size": 8,
        "min_quantity": Decimal("0.0001"),
        "slippage_factor": Decimal("0.999"),
    },
    "ETH": {
        "amount": ETH_AMOUNT,
        "symbol": "ETHGUSD",
        "tick_size": 6,
        "min_quantity": Decimal("0.00001"),
        "slippage_factor": Decimal("0.998"),
    },
}

# ----------------------------
# AWS clients / logging
# ----------------------------

ssm_client = boto3.client("ssm")
sns_client = boto3.client("sns")
SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN")

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def send_alert(subject: str, message: str) -> None:
    """Send an SNS alert if configured."""
    if not SNS_TOPIC_ARN:
        logger.warning("SNS topic ARN not set, skipping alert.")
        return
    try:
        sns_client.publish(TopicArn=SNS_TOPIC_ARN, Subject=subject, Message=message)
        logger.info(f"Alert sent: {subject}")
    except Exception as e:
        logger.error(f"Failed to send SNS alert: {str(e)}")


def get_api_keys():
    try:
        response = ssm_client.get_parameter(Name="GeminiApiKeys", WithDecryption=True)
        secret = json.loads(response["Parameter"]["Value"])
        return secret["API key"], secret["API Secret"]
    except Exception as e:
        raise ValueError(f"Error retrieving API keys from AWS SSM Parameter Store: {str(e)}")


def _get_gusd_available(gemini: GeminiClient) -> Decimal:
    balances = gemini.get_balance()
    for asset_info in balances:
        if asset_info.get("currency") == "GUSD":
            return Decimal(str(asset_info.get("available", "0")))
    return Decimal("0")


def buy_crypto(
    gemini: GeminiClient,
    asset: str,
    buy_size: Decimal,
    symbol: str,
    tick_size: int,
    min_quantity: Decimal,
    slippage_factor: Decimal,
):
    try:
        gusd_balance = _get_gusd_available(gemini)
        logger.info(f"GUSD Available Balance: ${gusd_balance}")
    except requests.exceptions.HTTPError as http_err:
        try:
            error_resp = http_err.response.json()
            reason = error_resp.get("reason")
            msg = error_resp.get("message") or str(error_resp)
            if reason == "ApiKeyIpFilteringFailure":
                error_message = (
                    f"API key blocked due to IP filtering for {asset}. "
                    "Update Gemini IP allowlist or disable IP restrictions."
                )
                logger.error(error_message)
                send_alert("Crypto Buy Failed - IP Filtering", error_message)
                return {"error": error_message}
            error_msg = reason or msg
        except Exception:
            error_msg = str(http_err)

        logger.error(f"Balance check failed for {asset}: {error_msg}")
        send_alert("Crypto Buy Failed - Balance Check", error_msg)
        return {"error": error_msg}
    except Exception as e:
        logger.error(f"Balance check failed for {asset}: {str(e)}")
        send_alert("Crypto Buy Failed - Balance Check", str(e))
        return {"error": str(e)}

    # Ensure never spend more than MAX_BUY
    required_funds = (buy_size * (Decimal("1") + FEE_RATE)).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
    if gusd_balance < required_funds:
        error_message = (
            f"Insufficient GUSD balance for {asset}: ${gusd_balance} available, need ${required_funds}. "
            "Fund your GUSD account or reduce TOTAL_DEPOSIT."
        )
        logger.error(error_message)
        send_alert("Crypto Buy Failed - Insufficient Funds", error_message)
        return {"error": error_message}

    # Get current ask price
    try:
        ticker = gemini.get_ticker(symbol)
        symbol_spot_price = Decimal(str(ticker["ask"]))
        logger.info(f"Spot Ask Price for {symbol}: ${symbol_spot_price} GUSD")
    except Exception as e:
        error_message = f"Failed to get ticker for {symbol}: {str(e)}"
        logger.error(error_message)
        send_alert("Crypto Buy Failed - Ticker", error_message)
        return {"error": error_message}

    # Execution price applies slippage
    execution_price = (symbol_spot_price * slippage_factor).quantize(Decimal("0.01"), rounding=ROUND_DOWN)

    # Calculate crypto amount to buy including fee
    crypto_amount = (buy_size * (Decimal("1") - FEE_RATE)) / execution_price

    # Quantize crypto amount to tick size (decimal places)
    quant = Decimal("1").scaleb(-tick_size)
    crypto_amount = crypto_amount.quantize(quant, rounding=ROUND_DOWN)

    if crypto_amount < min_quantity:
        error_message = (
            f"Calculated {asset} amount ({crypto_amount} {asset}) is below minimum order size ({min_quantity} {asset})."
        )
        logger.error(error_message)
        send_alert("Crypto Buy Failed - Order Too Small", error_message)
        return {"error": error_message}

    order_cost = (execution_price * crypto_amount).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
    order_fee = (order_cost * FEE_RATE).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
    total_order_cost = (order_cost + order_fee).quantize(Decimal("0.01"), rounding=ROUND_DOWN)

    logger.info(f"Order: {crypto_amount} {asset} at ${execution_price} GUSD = ${order_cost}")
    logger.info(f"Estimated fee: ${order_fee}")
    logger.info(f"Total order cost: ${total_order_cost} GUSD")

    order_payload = {
        "symbol": symbol,
        "amount": str(crypto_amount),
        "price": str(execution_price),
        "side": "buy",
        "type": "exchange limit",
        "options": ["maker-or-cancel"],
    }

    try:
        result = gemini.place_order(order_payload)
        logger.info(f"Maker Buy for {asset}: {result}")
        return result
    except requests.exceptions.HTTPError as http_err:
        try:
            error_resp = http_err.response.json()
            error_msg = error_resp.get("reason") or error_resp.get("message") or str(error_resp)
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
    """
    Goal:
      - Spend at most MAX_BUY per execution (half of TOTAL_DEPOSIT)
      - Keep fee calculations
      - Never touch the other half (reserve)
    """
    try:
        # Build Gemini client once
        public_key, private_key = get_api_keys()
        gemini = GeminiClient(public_key, private_key)

        # Total required funds computed from the NET amounts + fee
        total_required_funds = sum(
            (cfg["amount"] * (Decimal("1") + FEE_RATE)).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
            for cfg in BUY_CONFIG.values()
        )

        logger.info(f"Configured MAX_BUY: ${MAX_BUY}")
        logger.info(f"Configured net total: ${TOTAL_NET}")
        logger.info(f"Total required funds (net + fee): ${total_required_funds}")

        # Check GUSD balance once
        try:
            gusd_balance = _get_gusd_available(gemini)
            logger.info(f"GUSD Available Balance: ${gusd_balance}")

            if gusd_balance < MAX_BUY:
                error_message = (
                    f"Insufficient GUSD balance for all purchases: ${gusd_balance} available, "
                    f"max spend allowed: ${MAX_BUY}. Fund your GUSD account."
                )
                logger.error(error_message)
                send_alert("Crypto Buy Failed - Insufficient Total Funds", error_message)
                return {"statusCode": 400, "body": json.dumps({"error": error_message})}

        except requests.exceptions.HTTPError as http_err:
            try:
                error_resp = http_err.response.json()
                reason = error_resp.get("reason")
                msg = error_resp.get("message") or str(error_resp)
                if reason == "ApiKeyIpFilteringFailure":
                    error_message = (
                        "API key blocked due to IP filtering. Update Gemini IP allowlist or disable IP restrictions."
                    )
                    logger.error(error_message)
                    send_alert("Crypto Buy Failed - IP Filtering", error_message)
                    return {"statusCode": 400, "body": json.dumps({"error": error_message})}
                error_msg = reason or msg
            except Exception:
                error_msg = str(http_err)

            logger.error(f"Balance check failed: {error_msg}")
            send_alert("Crypto Buy Failed - Balance Check", error_msg)
            return {"statusCode": 400, "body": json.dumps({"error": error_msg})}

        # Execute buys
        results = {}
        for asset, cfg in BUY_CONFIG.items():
            results[asset] = buy_crypto(
                gemini=gemini,
                asset=asset,
                buy_size=cfg["amount"],
                symbol=cfg["symbol"],
                tick_size=cfg["tick_size"],
                min_quantity=cfg["min_quantity"],
                slippage_factor=cfg["slippage_factor"],
            )

        if any(isinstance(r, dict) and "error" in r for r in results.values()):
            send_alert("Crypto Buy Completed With Errors", json.dumps(results, indent=2))

        return {"statusCode": 200, "body": json.dumps(results)}

    except Exception as e:
        logger.error(f"Lambda execution failed: {str(e)}")
        send_alert("Crypto Buy Lambda Failed", f"Unhandled Error: {str(e)}")
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}