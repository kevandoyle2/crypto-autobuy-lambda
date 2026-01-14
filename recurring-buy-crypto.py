import json
import logging
import boto3
import os
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
from shared.gemini_client import GeminiClient

# ----------------------------
# Configuration
# ----------------------------

TOTAL_DEPOSIT = Decimal("170")
MAX_BUY = (TOTAL_DEPOSIT / 2).quantize(Decimal("0.01"))

BTC_PERCENTAGE = Decimal("66")
ETH_PERCENTAGE = Decimal("34")

# Gross allocations (total GUSD spent including fee)
BTC_AMOUNT = (MAX_BUY * (BTC_PERCENTAGE / 100)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
ETH_AMOUNT = (MAX_BUY - BTC_AMOUNT).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

BUY_CONFIG = {
    "BTC": {
        "amount": BTC_AMOUNT,
        "symbol": "btcgusd",
        "tick_size": 8,
        "min_quantity": Decimal("0.00001"),
        "slippage_factor": Decimal("0.999"),
    },
    "ETH": {
        "amount": ETH_AMOUNT,
        "symbol": "ethgusd",
        "tick_size": 6,
        "min_quantity": Decimal("0.001"),
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
    gross_amount: Decimal,
    symbol: str,
    tick_size: int,
    min_quantity: Decimal,
    slippage_factor: Decimal,
    fee_rate: Decimal,
):
    try:
        gusd_balance = _get_gusd_available(gemini)
        logger.info(f"GUSD Available Balance: ${gusd_balance}")
    except Exception as e:
        error_message = f"Balance check failed for {asset}: {str(e)}"
        logger.error(error_message)
        send_alert("Crypto Buy Failed - Balance Check", error_message)
        return {"error": error_message}

    if gusd_balance < gross_amount:
        error_message = f"Insufficient GUSD balance for {asset}: ${gusd_balance} available, need ${gross_amount}."
        logger.error(error_message)
        send_alert("Crypto Buy Failed - Insufficient Funds", error_message)
        return {"error": error_message}

    try:
        ticker = gemini.get_ticker(symbol)
        spot_price = Decimal(str(ticker["ask"]))
        logger.info(f"Spot Ask Price for {symbol}: ${spot_price}")
    except Exception as e:
        error_message = f"Failed to get ticker for {symbol}: {str(e)}"
        logger.error(error_message)
        send_alert("Crypto Buy Failed - Ticker", error_message)
        return {"error": error_message}

    execution_price = (spot_price * slippage_factor).quantize(Decimal("0.01"), rounding=ROUND_DOWN)

    # Calculate principal so principal + fee ≈ gross_amount
    principal_usd = (gross_amount / (Decimal("1") + fee_rate)).quantize(Decimal("0.01"), rounding=ROUND_DOWN)

    crypto_amount = (principal_usd / execution_price).quantize(
        Decimal("1").scaleb(-tick_size), rounding=ROUND_DOWN
    )

    if crypto_amount < min_quantity:
        error_message = f"Calculated {asset} amount ({crypto_amount}) is below minimum ({min_quantity})."
        logger.error(error_message)
        send_alert("Crypto Buy Failed - Order Too Small", error_message)
        return {"error": error_message}

    # Final actual costs after initial quantization
    order_cost = (crypto_amount * execution_price).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
    order_fee = (order_cost * fee_rate).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
    total_order_cost = (order_cost + order_fee).quantize(Decimal("0.01"), rounding=ROUND_DOWN)

    logger.info(f"Initial: {crypto_amount} {asset} -> Total: ${total_order_cost} (target: ${gross_amount})")

    # RELAXED BUMP LOOP — allows up to +$0.01 over to hit exact target
    tick = Decimal("1").scaleb(-tick_size)
    initial_crypto = crypto_amount

    while total_order_cost < gross_amount:
        potential_crypto = crypto_amount + tick
        potential_cost = (potential_crypto * execution_price).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
        potential_fee = (potential_cost * fee_rate).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
        potential_total = potential_cost + potential_fee

        # Allow tiny over-spend (max +$0.01) to reach or exceed target
        if potential_total > gross_amount + Decimal("0.01"):
            break

        crypto_amount = potential_crypto
        order_cost = potential_cost
        order_fee = potential_fee
        total_order_cost = potential_total

    if crypto_amount > initial_crypto:
        over = total_order_cost - gross_amount
        logger.info(f"Bumped {asset} to {crypto_amount} -> Total spend: ${total_order_cost} "
                    f"(exact or +${over:.2f})")

    logger.info(f"Final Order: {crypto_amount} {asset} at ${execution_price}")
    logger.info(f"Principal: ${order_cost}, Fee: ${order_fee}, Total spent: ${total_order_cost} (target: ${gross_amount})")

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
    except Exception as e:
        error_message = f"Order failed for {asset}: {str(e)}"
        logger.error(error_message)
        send_alert(f"Crypto Buy Failed - {asset}", error_message)
        return {"error": error_message}


def lambda_handler(event, context):
    try:
        public_key, private_key = get_api_keys()
        gemini = GeminiClient(public_key, private_key)

        # Dynamically fetch your actual maker fee tier
        try:
            notional_volume = gemini.get_notional_volume()
            maker_bps = int(notional_volume.get('api_maker_fee_bps', 20))  # Default low-volume tier
            FEE_RATE = Decimal(maker_bps) / Decimal("10000")
            logger.info(f"Dynamic Maker Fee Rate: {FEE_RATE * 100:.2f}% ({maker_bps} bps)")
        except Exception as e:
            error_message = f"Failed to fetch fee tier: {str(e)}. Using default 0.20%."
            logger.warning(error_message)
            send_alert("Fee Rate Fetch Warning", error_message)
            FEE_RATE = Decimal("0.002")  # 0.20%

        # Check overall balance
        gusd_balance = _get_gusd_available(gemini)
        if gusd_balance < MAX_BUY:
            error_message = f"Insufficient GUSD: ${gusd_balance} < ${MAX_BUY} required."
            logger.error(error_message)
            send_alert("Crypto Buy Failed - Insufficient Funds", error_message)
            return {"statusCode": 400, "body": json.dumps({"error": error_message})}

        # Execute buys
        results = {}
        for asset, cfg in BUY_CONFIG.items():
            results[asset] = buy_crypto(
                gemini=gemini,
                asset=asset,
                gross_amount=cfg["amount"],
                symbol=cfg["symbol"],
                tick_size=cfg["tick_size"],
                min_quantity=cfg["min_quantity"],
                slippage_factor=cfg["slippage_factor"],
                fee_rate=FEE_RATE,
            )

        if any("error" in r for r in results.values()):
            send_alert("Crypto Buy Completed With Errors", json.dumps(results, indent=2))

        return {"statusCode": 200, "body": json.dumps(results)}

    except Exception as e:
        logger.error(f"Lambda execution failed: {str(e)}")
        send_alert("Crypto Buy Lambda Failed", str(e))
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}