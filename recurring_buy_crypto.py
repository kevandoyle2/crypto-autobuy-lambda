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

BTC_AMOUNT = (MAX_BUY * (BTC_PERCENTAGE / 100)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
ETH_AMOUNT = (MAX_BUY - BTC_AMOUNT).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

BUY_CONFIG = {
    "BTC": {
        "amount": BTC_AMOUNT,
        "symbol": "btcgusd",
        "tick_size": 8,
        "min_quantity": Decimal("0.00001"),
        "slippage_factor": Decimal("0.999"),
        "maker_or_cancel": True,
    },
    "ETH": {
        "amount": ETH_AMOUNT,
        "symbol": "ethgusd",
        "tick_size": 6,
        "min_quantity": Decimal("0.001"),
        "slippage_factor": Decimal("1.0001"),  # Slight premium above ask — forces acceptance
        "maker_or_cancel": False,  # No maker-or-cancel — bypasses 406 validation
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
        raise ValueError(f"Error retrieving API keys: {str(e)}")


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
    maker_or_cancel: bool,
):
    gusd_balance = _get_gusd_available(gemini)
    logger.info(f"GUSD Available Balance: ${gusd_balance}")

    effective_gross = min(gross_amount, gusd_balance)
    SHORTFALL_TOLERANCE = Decimal("0.20")
    if effective_gross < gross_amount - SHORTFALL_TOLERANCE:
        error_message = f"Insufficient GUSD for {asset}: ${gusd_balance} available, need ${gross_amount} (short by too much)"
        logger.error(error_message)
        send_alert("Crypto Buy Failed - Insufficient Funds", error_message)
        return {"error": error_message}

    if effective_gross < gross_amount:
        logger.info(f"Adjusted {asset} target from ${gross_amount} to ${effective_gross} (available balance)")

    try:
        ticker = gemini.get_ticker(symbol)
        spot_price = Decimal(str(ticker["ask"]))
        logger.info(f"Spot Ask Price for {symbol}: ${spot_price}")
    except Exception as e:
        error_message = f"Failed to get ticker for {symbol}: {str(e)}"
        logger.error(error_message)
        send_alert("Crypto Buy Failed - Ticker", error_message)
        return {"error": error_message}

    execution_price = (spot_price * slippage_factor).quantize(Decimal("0.01"), ROUND_DOWN)

    principal_usd = (effective_gross / (Decimal("1") + fee_rate)).quantize(Decimal("0.01"), ROUND_DOWN)

    crypto_amount = (principal_usd / execution_price).quantize(
        Decimal("1").scaleb(-tick_size), rounding=ROUND_DOWN
    )

    if crypto_amount < min_quantity:
        error_message = f"Calculated {asset} amount ({crypto_amount}) is below minimum ({min_quantity})."
        logger.error(error_message)
        send_alert("Crypto Buy Failed - Order Too Small", error_message)
        return {"error": error_message}

    estimated_quote = (crypto_amount * execution_price).quantize(Decimal("0.01"))
    MIN_QUOTE_VALUE = Decimal("15.00") if asset == "ETH" else Decimal("10.00")
    if estimated_quote < MIN_QUOTE_VALUE:
        error_message = f"Estimated {asset} order too small (${estimated_quote}) — skipping"
        logger.error(error_message)
        send_alert("Crypto Buy Failed - Order Too Small", error_message)
        return {"error": error_message}

    order_cost = (crypto_amount * execution_price).quantize(Decimal("0.01"), ROUND_DOWN)
    order_fee = (order_cost * fee_rate).quantize(Decimal("0.01"), ROUND_DOWN)
    total_order_cost = (order_cost + order_fee).quantize(Decimal("0.01"), ROUND_DOWN)

    logger.info(f"Initial: {crypto_amount} {asset} -> Total: ${total_order_cost} (effective target: ${effective_gross})")

    # STRICT BUMP LOOP — maximum possible without ever going over
    tick = Decimal("1").scaleb(-tick_size)
    initial_crypto = crypto_amount

    while True:
        potential_crypto = crypto_amount + tick
        potential_cost = (potential_crypto * execution_price).quantize(Decimal("0.01"), ROUND_DOWN)
        potential_fee = (potential_cost * fee_rate).quantize(Decimal("0.01"), ROUND_DOWN)
        potential_total = potential_cost + potential_fee

        if potential_total > effective_gross:
            break

        crypto_amount = potential_crypto
        order_cost = potential_cost
        order_fee = potential_fee
        total_order_cost = potential_total

    if crypto_amount > initial_crypto:
        under = effective_gross - total_order_cost
        logger.info(f"Bumped {asset} to {crypto_amount} -> Total spend: ${total_order_cost} "
                    f"(exact or under by ${under:.2f})")

    logger.info(f"Final Order: {crypto_amount} {asset} at ${execution_price}")
    logger.info(f"Principal: ${order_cost}, Fee: ${order_fee}, Total spent: ${total_order_cost} "
                f"(original target: ${gross_amount}, effective: ${effective_gross})")

    order_payload = {
        "symbol": symbol,
        "amount": str(crypto_amount),
        "price": str(execution_price),
        "side": "buy",
        "type": "exchange limit",
    }

    if maker_or_cancel:
        order_payload["options"] = ["maker-or-cancel"]

    logger.info(f"Placing order for {asset}: {json.dumps(order_payload, indent=2)}")

    try:
        result = gemini.place_order(order_payload)
        logger.info(f"Order placed for {asset}: {result}")
        return result
    except Exception as e:
        full_error = str(e)
        logger.error(f"Order failed for {asset}: {full_error}")
        send_alert(f"Crypto Buy Failed - {asset}", full_error)
        return {"error": full_error}


def lambda_handler(event, context):
    try:
        public_key, private_key = get_api_keys()
        gemini = GeminiClient(public_key, private_key)

        try:
            notional_volume = gemini.get_notional_volume()
            maker_bps = int(notional_volume.get('api_maker_fee_bps', 20))
            FEE_RATE = Decimal(maker_bps) / Decimal("10000")
            logger.info(f"Dynamic Maker Fee Rate: {FEE_RATE * 100:.2f}% ({maker_bps} bps)")
        except Exception as e:
            error_message = f"Failed to fetch fee tier: {str(e)}. Using default 0.20%."
            logger.warning(error_message)
            send_alert("Fee Rate Fetch Warning", error_message)
            FEE_RATE = Decimal("0.002")

        gusd_balance = _get_gusd_available(gemini)
        if gusd_balance < Decimal("20.00"):
            error_message = f"Insufficient GUSD: ${gusd_balance} (too low to proceed)"
            logger.error(error_message)
            send_alert("Crypto Buy Failed - Insufficient Funds", error_message)
            return {"statusCode": 400, "body": json.dumps({"error": error_message})}

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
                maker_or_cancel=cfg.get("maker_or_cancel", True),
            )

        if any("error" in r for r in results.values()):
            send_alert("Crypto Buy Completed With Errors", json.dumps(results, indent=2))

        return {"statusCode": 200, "body": json.dumps(results)}

    except Exception as e:
        logger.error(f"Lambda execution failed: {str(e)}")
        send_alert("Crypto Buy Lambda Failed", str(e))
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}