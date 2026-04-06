import json
import logging
import boto3
import os
import concurrent.futures
from decimal import Decimal, ROUND_HALF_UP

from shared.gemini_client import GeminiClient
from shared.crypto_buy_engine import execute_buy

# ============================================================
# CONFIGURATION
# ============================================================

GUSD_FLOOR = Decimal("1.00")

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
        "price_tick": Decimal("0.01"),
    },
    "ETH": {
        "symbol": "ethgusd",
        "amount": ETH_AMOUNT,
        "tick_size": 6,
        "min_quantity": Decimal("0.001"),
        "price_tick": Decimal("0.01"),
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
        sns_client.publish(TopicArn=SNS_TOPIC_ARN, Subject=subject, Message=message)
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

# ============================================================
# LAMBDA HANDLER
# ============================================================

def lambda_handler(event, context=None):
    try:
        public_key, private_key = get_api_keys()
        gemini = GeminiClient(public_key, private_key)

        gusd_balance = get_gusd_balance(gemini)
        required_balance = (MAX_BUY + GUSD_FLOOR).quantize(Decimal("0.01"))

        if gusd_balance < required_balance:
            summary = {
                "classification": "Skipped",
                "reason": "Insufficient funds for full scheduled buy",
                "balance": str(gusd_balance),
                "required_balance": str(required_balance),
            }
            send_alert("Crypto Buy Lambda - Skipped (Insufficient Funds)", json.dumps(summary, indent=2))
            return {"statusCode": 200, "body": json.dumps(summary, indent=2)}

        # Fetch fees (fail safe to 60bps maker / 120bps taker - Gemini base tier)
        try:
            nv = gemini.get_notional_volume()
            maker_fee = Decimal(str(nv.get("api_maker_fee_bps", 60))) / Decimal("10000")
            taker_fee = Decimal(str(nv.get("api_taker_fee_bps", 120))) / Decimal("10000")
        except Exception:
            maker_fee = Decimal("0.006")
            taker_fee = Decimal("0.012")
            logger.warning("Fee fetch failed — defaulting to 0.60% maker / 1.20% taker")

        logger.info(f"Maker fee: {maker_fee * 100:.4f}%  Taker fee: {taker_fee * 100:.4f}%")

        # Place both orders concurrently to minimize book-movement risk
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            fut_btc = executor.submit(execute_buy, gemini, "BTC", BUY_CONFIG["BTC"], maker_fee, taker_fee, gusd_balance)
            fut_eth = executor.submit(execute_buy, gemini, "ETH", BUY_CONFIG["ETH"], maker_fee, taker_fee, gusd_balance)
            results = {
                "BTC": fut_btc.result(),
                "ETH": fut_eth.result(),
            }

        # Classify outcome
        statuses = []
        for asset, result in results.items():
            if result.get("error"):
                statuses.append("error")
            elif result.get("skipped"):
                statuses.append("skipped")
            elif "order_id" in result:
                statuses.append("placed")
            else:
                statuses.append("unknown")

        if all(s == "placed" for s in statuses):
            classification = "Success"
        elif all(s == "skipped" for s in statuses):
            classification = "Skipped"
        elif "placed" in statuses and "skipped" in statuses:
            classification = "Partial"
        elif "error" in statuses:
            classification = "Error"
        else:
            classification = "Unknown"

        summary = {
            "classification": classification,
            "balance": str(gusd_balance),
            "maker_fee_bps": str(maker_fee * 10000),
            "taker_fee_bps": str(taker_fee * 10000),
            "results": results,
        }

        send_alert(f"Crypto Buy Lambda - {classification}", json.dumps(summary, indent=2))
        return {"statusCode": 200, "body": json.dumps(summary, indent=2)}

    except Exception as e:
        logger.exception("Lambda execution failed")
        send_alert("Crypto Buy Lambda - Error", str(e))
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}