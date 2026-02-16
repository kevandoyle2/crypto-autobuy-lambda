import json
import logging
import boto3
import os
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
from shared.gemini_client import GeminiClient

# ============================================================
# CONFIGURATION — FIXED WEEKLY BUY AMOUNTS
# ============================================================

TOTAL_DEPOSIT = Decimal("170")
MAX_BUY = (TOTAL_DEPOSIT / 2).quantize(Decimal("0.01"))

BTC_PERCENTAGE = Decimal("66")
ETH_PERCENTAGE = Decimal("34")

BTC_AMOUNT = (MAX_BUY * BTC_PERCENTAGE / 100).quantize(Decimal("0.01"), ROUND_HALF_UP)
ETH_AMOUNT = (MAX_BUY - BTC_AMOUNT).quantize(Decimal("0.01"), ROUND_HALF_UP)

BUY_CONFIG = {
    "BTC": {
        "symbol": "btcgusd",
        "amount": BTC_AMOUNT,
        "tick_size": 8,
        "min_quantity": Decimal("0.00001"),
    },
    "ETH": {
        "symbol": "ethgusd",
        "amount": ETH_AMOUNT,
        "tick_size": 6,
        "min_quantity": Decimal("0.001"),
    },
}

# ============================================================
# AWS CLIENTS + LOGGING
# ============================================================

ssm_client = boto3.client("ssm")
sns_client = boto3.client("sns")
SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# ============================================================
# ALERTING
# ============================================================

def send_alert(subject: str, message: str):
    if not SNS_TOPIC_ARN:
        return

    try:
        sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=subject,
            Message=message
        )
    except Exception as e:
        logger.error(f"SNS failed: {e}")


# ============================================================
# HELPERS
# ============================================================

def get_api_keys():
    try:
        response = ssm_client.get_parameter(
            Name="GeminiApiKeys",
            WithDecryption=True
        )
        secret = json.loads(response["Parameter"]["Value"])
        return secret["API key"], secret["API Secret"]
    except Exception as e:
        raise Exception(f"Failed retrieving API keys: {e}")


def get_gusd_balance(gemini: GeminiClient) -> Decimal:
    balances = gemini.get_balance()
    for b in balances:
        if b.get("currency") == "GUSD":
            return Decimal(str(b.get("available", "0")))
    return Decimal("0")


# ============================================================
# CORE ORDER ENGINE
# ============================================================

def execute_buy(gemini, asset_name, config, maker_fee):

    gross_amount = config["amount"]
    symbol = config["symbol"]

    book = gemini.get_book(symbol)

    best_bid = Decimal(str(book["bids"][0]["price"]))
    best_ask = Decimal(str(book["asks"][0]["price"]))

    tick = Decimal("1").scaleb(-config["tick_size"])

    def compute_quantity(price, fee_rate):
        principal = (gross_amount / (Decimal("1") + fee_rate)) \
            .quantize(Decimal("0.01"), ROUND_DOWN)

        quantity = (principal / price) \
            .quantize(tick, rounding=ROUND_DOWN)

        return quantity

    # ====================================================
    # 1️⃣ MAKER ATTEMPT (LOWEST FEES)
    # ====================================================

    maker_price = (best_bid - Decimal("0.01")) \
        .quantize(Decimal("0.01"), ROUND_DOWN)

    qty = compute_quantity(maker_price, maker_fee)

    if qty >= config["min_quantity"]:
        payload = {
            "symbol": symbol,
            "amount": str(qty),
            "price": str(maker_price),
            "side": "buy",
            "type": "exchange limit",
            "options": ["maker-or-cancel"],
        }

        try:
            logger.info(f"{asset_name}: Maker attempt @ {maker_price}")
            result = gemini.place_order(payload)
            return {"mode": "maker", "result": result}
        except Exception:
            logger.info(f"{asset_name}: Maker failed → fallback")

    # ====================================================
    # 2️⃣ TAKER FALLBACK
    # ====================================================

    taker_price = (best_ask + Decimal("0.01")) \
        .quantize(Decimal("0.01"), ROUND_HALF_UP)

    taker_fee = maker_fee * Decimal("2")

    qty = compute_quantity(taker_price, taker_fee)

    if qty < config["min_quantity"]:
        error_msg = f"{asset_name} below minimum trade size"
        logger.error(error_msg)
        return {"error": error_msg}

    payload = {
        "symbol": symbol,
        "amount": str(qty),
        "price": str(taker_price),
        "side": "buy",
        "type": "exchange limit",
    }

    logger.info(f"{asset_name}: Taker fallback @ {taker_price}")
    result = gemini.place_order(payload)

    return {"mode": "taker_fallback", "result": result}


# ============================================================
# LAMBDA HANDLER
# ============================================================

def lambda_handler(event, context):

    try:
        public_key, private_key = get_api_keys()
        gemini = GeminiClient(public_key, private_key)

        gusd_balance = get_gusd_balance(gemini)

        required_total = BTC_AMOUNT + ETH_AMOUNT

        if gusd_balance < required_total:
            message = (
                f"Insufficient GUSD. "
                f"Balance: {gusd_balance}, Required: {required_total}"
            )
            logger.error(message)
            send_alert("Crypto Buy Failed - Insufficient Funds", message)

            return {
                "statusCode": 400,
                "body": json.dumps({"error": message})
            }

        # ----------------------------------------
        # Fetch Dynamic Maker Fee Tier
        # ----------------------------------------

        try:
            nv = gemini.get_notional_volume()
            maker_bps = int(nv.get("api_maker_fee_bps", 20))
            maker_fee = Decimal(maker_bps) / Decimal("10000")
            logger.info(f"Maker fee: {maker_fee * 100:.4f}%")
        except Exception:
            maker_fee = Decimal("0.002")
            logger.warning("Fee fetch failed — using 0.20%")

        results = {}

        results["BTC"] = execute_buy(
            gemini,
            "BTC",
            BUY_CONFIG["BTC"],
            maker_fee
        )

        results["ETH"] = execute_buy(
            gemini,
            "ETH",
            BUY_CONFIG["ETH"],
            maker_fee
        )

        # Send alert if partial failures
        if any("error" in r for r in results.values()):
            send_alert("Crypto Buy Completed With Errors",
                       json.dumps(results, indent=2))

        return {
            "statusCode": 200,
            "body": json.dumps({
                "balance": str(gusd_balance),
                "btc_target": str(BTC_AMOUNT),
                "eth_target": str(ETH_AMOUNT),
                "results": results
            })
        }

    except Exception as e:
        logger.exception("Lambda execution failed")
        send_alert("Crypto Buy Lambda Fatal Error", str(e))

        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }