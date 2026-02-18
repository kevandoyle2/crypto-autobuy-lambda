import json
import logging
import boto3
import os
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
from shared.gemini_client import GeminiClient

# ============================================================
# CONFIGURATION
# ============================================================

GUSD_FLOOR = Decimal("1.00")  # Hard floor: never spend below this

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
    response = ssm_client.get_parameter(Name="GeminiApiKeys", WithDecryption=True)
    secret = json.loads(response["Parameter"]["Value"])
    return secret["API key"], secret["API Secret"]

def get_gusd_balance(gemini: GeminiClient) -> Decimal:
    for b in gemini.get_balance():
        if b.get("currency") == "GUSD":
            return Decimal(str(b.get("available", "0")))
    return Decimal("0")

def _quant_step(tick_size: int) -> Decimal:
    return Decimal("1").scaleb(-tick_size)

def _compute_totals(price: Decimal, qty: Decimal, fee_rate: Decimal):
    cost = (price * qty).quantize(Decimal("0.01"), ROUND_DOWN)
    fee = (cost * fee_rate).quantize(Decimal("0.01"), ROUND_DOWN)
    total = (cost + fee).quantize(Decimal("0.01"), ROUND_DOWN)
    return cost, fee, total

# ============================================================
# CORE ORDER ENGINE
# ============================================================

def execute_buy(gemini, asset_name, config, maker_fee, gusd_balance):
    gross_amount = config["amount"]

    # enforce GUSD floor
    available_to_spend = max(Decimal("0"), (gusd_balance - GUSD_FLOOR).quantize(Decimal("0.01"), ROUND_DOWN))
    gross_amount = min(gross_amount, available_to_spend)
    if gross_amount <= Decimal("0"):
        logger.info(f"{asset_name}: Skipped (floor prevents spend)")
        return {"skipped": True, "reason": "GUSD floor prevents spend"}

    symbol = config["symbol"]
    book = gemini.get_book(symbol)
    best_bid = Decimal(str(book["bids"][0]["price"]))
    best_ask = Decimal(str(book["asks"][0]["price"]))
    tick = _quant_step(config["tick_size"])

    def compute_qty(price, fee_rate):
        principal = (gross_amount / (Decimal("1") + fee_rate)).quantize(Decimal("0.01"), ROUND_HALF_UP)
        qty = (principal / price).quantize(tick, ROUND_DOWN)
        return qty

    # 1️⃣ MAKER ATTEMPT
    maker_price = (best_bid - Decimal("0.01")).quantize(Decimal("0.01"), ROUND_DOWN)
    qty = compute_qty(maker_price, maker_fee)

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

    # 2️⃣ TAKER FALLBACK
    taker_price = (best_ask + Decimal("0.01")).quantize(Decimal("0.01"), ROUND_HALF_UP)
    taker_fee = maker_fee * Decimal("2")
    qty = compute_qty(taker_price, taker_fee)

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

def lambda_handler(event, context=None):
    try:
        public_key, private_key = get_api_keys()
        gemini = GeminiClient(public_key, private_key)

        gusd_balance = get_gusd_balance(gemini)

        # Fetch dynamic maker fee
        try:
            nv = gemini.get_notional_volume()
            maker_fee = Decimal(int(nv.get("api_maker_fee_bps", 20))) / Decimal("10000")
        except Exception:
            maker_fee = Decimal("0.002")
            logger.warning("Fee fetch failed — using 0.20%")

        results = {}
        results["BTC"] = execute_buy(gemini, "BTC", BUY_CONFIG["BTC"], maker_fee, gusd_balance)
        results["ETH"] = execute_buy(gemini, "ETH", BUY_CONFIG["ETH"], maker_fee, gusd_balance)

        return {
            "statusCode": 200,
            "body": json.dumps({
                "balance": str(gusd_balance),
                "results": results
            }, indent=2)
        }

    except Exception as e:
        logger.exception("Lambda execution failed")
        send_alert("Crypto Buy Lambda Fatal Error", str(e))
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }