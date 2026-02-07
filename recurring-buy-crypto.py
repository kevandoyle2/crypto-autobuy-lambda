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

# Gross allocations (target total GUSD spent per asset INCLUDING fee)
BTC_AMOUNT = (MAX_BUY * (BTC_PERCENTAGE / Decimal("100"))).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
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
    response = ssm_client.get_parameter(Name="GeminiApiKeys", WithDecryption=True)
    secret = json.loads(response["Parameter"]["Value"])
    return secret["API key"], secret["API Secret"]


def _get_gusd_available(gemini: GeminiClient) -> Decimal:
    for asset_info in gemini.get_balance():
        if asset_info.get("currency") == "GUSD":
            return Decimal(str(asset_info.get("available", "0")))
    return Decimal("0")


def _quant_step(tick_size: int) -> Decimal:
    return Decimal("1").scaleb(-tick_size)


def _compute_totals(execution_price: Decimal, crypto_amount: Decimal, fee_rate: Decimal):
    """
    Deterministic cent math:
      cost = floor(price * qty, cents)
      fee  = floor(cost * fee_rate, cents)
      total= floor(cost + fee, cents)
    """
    order_cost = (execution_price * crypto_amount).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
    order_fee = (order_cost * fee_rate).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
    total_cost = (order_cost + order_fee).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
    return order_cost, order_fee, total_cost


def plan_order_to_cap(
    *,
    gemini: GeminiClient,
    asset: str,
    gross_cap: Decimal,
    symbol: str,
    tick_size: int,
    min_quantity: Decimal,
    slippage_factor: Decimal,
    fee_rate: Decimal,
) -> dict:
    """
    Create an order plan that maximizes spend while strictly enforcing:
      total_cost <= gross_cap
    """
    if gross_cap <= Decimal("0.00"):
        return {"skipped": True, "reason": "gross_cap <= 0"}

    ticker = gemini.get_ticker(symbol)
    ask = Decimal(str(ticker["ask"]))
    execution_price = (ask * slippage_factor).quantize(Decimal("0.01"), rounding=ROUND_DOWN)

    step = _quant_step(tick_size)

    # Size from gross directly so fee is included in cap
    crypto_amount = (gross_cap / (execution_price * (Decimal("1") + fee_rate))).quantize(step, rounding=ROUND_DOWN)

    if crypto_amount < min_quantity:
        return {"skipped": True, "reason": f"{asset}: below min quantity", "gross_cap": str(gross_cap)}

    order_cost, order_fee, total_cost = _compute_totals(execution_price, crypto_amount, fee_rate)

    # Bump UP while staying <= cap (STRICT: never exceed)
    while True:
        candidate = (crypto_amount + step).quantize(step, rounding=ROUND_DOWN)
        c_cost, c_fee, c_total = _compute_totals(execution_price, candidate, fee_rate)
        if c_total > gross_cap:
            break
        crypto_amount, order_cost, order_fee, total_cost = candidate, c_cost, c_fee, c_total

    return {
        "asset": asset,
        "gross_cap": str(gross_cap),
        "execution_price": str(execution_price),
        "crypto_amount": str(crypto_amount),
        "order_cost": str(order_cost),
        "order_fee": str(order_fee),
        "total_cost": str(total_cost),
        "order_payload": {
            "symbol": symbol,
            "amount": str(crypto_amount),
            "price": str(execution_price),
            "side": "buy",
            "type": "exchange limit",
            "options": ["maker-or-cancel"],
        },
    }


def place_planned_order(*, gemini: GeminiClient, plan: dict) -> dict:
    if plan.get("skipped"):
        return plan
    if "order_payload" not in plan:
        return {"error": "Invalid plan", "plan": plan}
    try:
        result = gemini.place_order(plan["order_payload"])
        return {"placed": True, "plan": plan, "result": result}
    except Exception as e:
        return {"error": str(e), "plan": plan}


def lambda_handler(event, context):
    try:
        public_key, private_key = get_api_keys()
        gemini = GeminiClient(public_key, private_key)

        # Dynamic maker fee tier
        try:
            notional_volume = gemini.get_notional_volume()
            maker_bps = int(notional_volume.get("api_maker_fee_bps", 20))
            fee_rate = Decimal(maker_bps) / Decimal("10000")
            logger.info(f"Dynamic Maker Fee Rate: {fee_rate * 100:.4f}% ({maker_bps} bps)")
        except Exception as e:
            msg = f"Failed to fetch fee tier: {str(e)}. Using fallback 0.20%."
            logger.warning(msg)
            send_alert("Fee Rate Fetch Warning", msg)
            maker_bps = 20
            fee_rate = Decimal("0.0020")

        # Balance check
        gusd_balance = _get_gusd_available(gemini).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
        logger.info(f"GUSD Available Balance: ${gusd_balance}")
        logger.info(f"Configured MAX_BUY: ${MAX_BUY} (BTC ${BTC_AMOUNT}, ETH ${ETH_AMOUNT})")

        if gusd_balance < MAX_BUY:
            error_message = f"Insufficient GUSD: ${gusd_balance} < ${MAX_BUY} required."
            logger.error(error_message)
            send_alert("Crypto Buy Failed - Insufficient Funds", error_message)
            return {"statusCode": 400, "body": json.dumps({"error": error_message})}

        # Plan + place orders
        results = {"fee_rate_bps": maker_bps, "fee_rate": str(fee_rate), "max_buy": str(MAX_BUY)}

        for asset, cfg in BUY_CONFIG.items():
            plan = plan_order_to_cap(
                gemini=gemini,
                asset=asset,
                gross_cap=cfg["amount"],
                symbol=cfg["symbol"],
                tick_size=cfg["tick_size"],
                min_quantity=cfg["min_quantity"],
                slippage_factor=cfg["slippage_factor"],
                fee_rate=fee_rate,
            )
            logger.info(f"{asset} planned total: ${plan.get('total_cost')} (cap ${cfg['amount']})")
            results[asset] = place_planned_order(gemini=gemini, plan=plan)

        if any(isinstance(results.get(a), dict) and "error" in results[a] for a in ("BTC", "ETH")):
            send_alert("Crypto Buy Completed With Errors", json.dumps(results, indent=2))

        return {"statusCode": 200, "body": json.dumps(results)}

    except Exception as e:
        logger.error(f"Lambda execution failed: {str(e)}")
        send_alert("Crypto Buy Lambda Failed", str(e))
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}