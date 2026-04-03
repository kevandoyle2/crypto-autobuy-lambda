import json
import logging
import boto3
import os
import concurrent.futures
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

# How many price ticks below best bid to sit.
# 1 tick is enough to be passive; more = slower fill but safer maker rate.
PASSIVE_TICKS_BELOW_BID = 1

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

def _quant_step(tick_size: int) -> Decimal:
    return Decimal("1").scaleb(-tick_size)

# ============================================================
# CORE ORDER ENGINE
# ============================================================

def execute_buy(gemini: GeminiClient, asset_name: str, config: dict, maker_fee: Decimal, gusd_balance: Decimal) -> dict:
    gross_amount = config["amount"]

    available_to_spend = max(
        Decimal("0"),
        (gusd_balance - GUSD_FLOOR).quantize(Decimal("0.01"), ROUND_DOWN),
    )
    gross_amount = min(gross_amount, available_to_spend)

    if gross_amount <= Decimal("0"):
        logger.info(f"{asset_name}: Skipped (GUSD floor prevents spend)")
        return {"skipped": True, "reason": "GUSD floor prevents spend"}

    symbol = config["symbol"]
    tick = _quant_step(config["tick_size"])
    price_tick = config["price_tick"]

    book = gemini.get_book(symbol)
    best_bid = Decimal(str(book["bids"][0]["price"]))
    best_ask = Decimal(str(book["asks"][0]["price"]))
    spread = best_ask - best_bid

    logger.info(f"{asset_name}: best_bid={best_bid} best_ask={best_ask} spread={spread}")

    # Sit 1 tick below best bid — passive, earmarks GUSD immediately.
    price = (best_bid - PASSIVE_TICKS_BELOW_BID * price_tick).quantize(price_tick, ROUND_DOWN)
    price = max(price_tick, price)

    # Fee-inclusive sizing: gross_amount is the hard ceiling including fees.
    # qty = floor(gross_amount / (price * (1 + maker_fee)), tick)
    # Guarantees cost + fee never exceeds gross_amount.
    qty = (gross_amount / (price * (Decimal("1") + maker_fee))).quantize(tick, ROUND_DOWN)

    estimated_fee = (price * qty * maker_fee).quantize(Decimal("0.01"), ROUND_DOWN)
    estimated_total = (price * qty + estimated_fee).quantize(Decimal("0.01"), ROUND_DOWN)

    logger.info(
        f"{asset_name}: qty={qty} price={price} "
        f"est_cost={price * qty:.2f} est_fee={estimated_fee} est_total={estimated_total} "
        f"budget={gross_amount}"
    )

    if qty < config["min_quantity"]:
        msg = f"{asset_name}: qty {qty} below minimum {config['min_quantity']}"
        logger.error(msg)
        return {"error": msg}

    # No maker-or-cancel — order rests on the book and reserves GUSD
    # until it fills naturally or is cancelled manually.
    payload = {
        "symbol": symbol,
        "amount": str(qty),
        "price": str(price),
        "side": "buy",
        "type": "exchange limit",
    }

    result = gemini.place_order(payload)
    order_id = result.get("order_id")
    logger.info(f"{asset_name}: Order placed, order_id={order_id}")

    return {
        "mode": "passive_limit",
        "order_id": order_id,
        "price": str(price),
        "qty": str(qty),
        "estimated_fee": str(estimated_fee),
        "estimated_total": str(estimated_total),
        "budget": str(gross_amount),
        "result": result,
    }

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

        # Fetch maker fee — fail safe to 20bps
        try:
            nv = gemini.get_notional_volume()
            maker_fee = Decimal(str(nv.get("api_maker_fee_bps", 20))) / Decimal("10000")
        except Exception:
            maker_fee = Decimal("0.002")
            logger.warning("Fee fetch failed — defaulting to 0.20%")

        logger.info(f"Maker fee: {maker_fee * 100:.4f}%")

        # Place both orders concurrently to minimize book-movement risk
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            fut_btc = executor.submit(execute_buy, gemini, "BTC", BUY_CONFIG["BTC"], maker_fee, gusd_balance)
            fut_eth = executor.submit(execute_buy, gemini, "ETH", BUY_CONFIG["ETH"], maker_fee, gusd_balance)
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
            "results": results,
        }

        send_alert(f"Crypto Buy Lambda - {classification}", json.dumps(summary, indent=2))
        return {"statusCode": 200, "body": json.dumps(summary, indent=2)}

    except Exception as e:
        logger.exception("Lambda execution failed")
        send_alert("Crypto Buy Lambda - Error", str(e))
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}