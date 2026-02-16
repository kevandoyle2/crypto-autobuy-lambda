import json
import logging
import boto3
import os
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP, ROUND_UP

import requests
from shared.gemini_client import GeminiClient

# ----------------------------
# Configuration
# ----------------------------

TOTAL_DEPOSIT = Decimal("170")
MAX_BUY = (TOTAL_DEPOSIT / 2).quantize(Decimal("0.01"))

BTC_PERCENTAGE = Decimal("66")
ETH_PERCENTAGE = Decimal("34")

# Gross allocations (total GUSD spent including fee)
BTC_AMOUNT = (MAX_BUY * (BTC_PERCENTAGE / Decimal("100"))).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
ETH_AMOUNT = (MAX_BUY - BTC_AMOUNT).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

ASSET_DEFAULTS = {
    "BTC": {
        "symbol": "btcgusd",
        "tick_size": 8,
        "min_quantity": Decimal("0.00001"),
        "slippage_factor": Decimal("0.999"),
    },
    "ETH": {
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
    balances = gemini.get_balance()
    for asset_info in balances:
        if asset_info.get("currency") == "GUSD":
            return Decimal(str(asset_info.get("available", "0")))
    return Decimal("0")


def _quant_step(tick_size: int) -> Decimal:
    return Decimal("1").scaleb(-tick_size)


def _get_execution_price(gemini: GeminiClient, symbol: str, slippage_factor: Decimal) -> Decimal:
    ticker = gemini.get_ticker(symbol)
    ask = Decimal(str(ticker["ask"]))
    return (ask * slippage_factor).quantize(Decimal("0.01"), rounding=ROUND_DOWN)


def _compute_costs_floor(execution_price: Decimal, crypto_amount: Decimal, fee_rate: Decimal):
    """
    Your original logging model (ROUND_DOWN).
    """
    order_cost = (execution_price * crypto_amount).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
    order_fee = (order_cost * fee_rate).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
    total_cost = (order_cost + order_fee).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
    return order_cost, order_fee, total_cost


def _required_funds_conservative(execution_price: Decimal, crypto_amount: Decimal, fee_rate: Decimal) -> Decimal:
    """
    Conservative funds check to avoid Gemini 'InsufficientFunds':
      - cost rounded DOWN to cents (what notional can be)
      - fee rounded UP to cents (more conservative than our estimate)
    """
    cost = (execution_price * crypto_amount).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
    fee = (cost * fee_rate).quantize(Decimal("0.01"), rounding=ROUND_UP)
    return (cost + fee).quantize(Decimal("0.01"), rounding=ROUND_UP)


def plan_order_to_cap(
    *,
    gemini: GeminiClient,
    asset: str,
    gross_cap: Decimal,
    available_gusd: Decimal,
    symbol: str,
    tick_size: int,
    min_quantity: Decimal,
    slippage_factor: Decimal,
    fee_rate: Decimal,
) -> dict:
    """
    Option 1 behavior:
      - Never overspend (<= gross_cap)
      - Spend as close as possible
      - ALSO must fit within available_gusd according to a conservative funds check
        (prevents Gemini InsufficientFunds when holds/rounding differ).
    """
    gross_cap = gross_cap.quantize(Decimal("0.01"), rounding=ROUND_DOWN)
    available_gusd = available_gusd.quantize(Decimal("0.01"), rounding=ROUND_DOWN)

    if gross_cap <= Decimal("0.00"):
        return {"skipped": True, "reason": "gross_cap <= 0"}

    if available_gusd <= Decimal("0.00"):
        return {"skipped": True, "reason": "available_gusd <= 0", "available_gusd": str(available_gusd)}

    # Effective cap cannot exceed available
    effective_cap = min(gross_cap, available_gusd).quantize(Decimal("0.01"), rounding=ROUND_DOWN)

    execution_price = _get_execution_price(gemini, symbol, slippage_factor)
    step = _quant_step(tick_size)

    # Start under cap: gross / (price * (1 + fee))
    crypto_amount = (effective_cap / (execution_price * (Decimal("1") + fee_rate))).quantize(step, rounding=ROUND_DOWN)

    if crypto_amount < min_quantity:
        return {
            "skipped": True,
            "reason": f"{asset}: below min quantity",
            "effective_cap": str(effective_cap),
            "execution_price": str(execution_price),
            "crypto_amount": str(crypto_amount),
        }

    # Bump while staying within BOTH caps:
    #  - total_cost_floor <= effective_cap (our non-overspend target)
    #  - required_funds_conservative <= available_gusd (Gemini acceptance safety)
    while True:
        cand = (crypto_amount + step).quantize(step, rounding=ROUND_DOWN)

        # Our "never overspend" check using floor model
        c_cost, c_fee, c_total = _compute_costs_floor(execution_price, cand, fee_rate)
        if c_total > effective_cap:
            break

        # Gemini acceptance safety check (fee rounded up)
        req = _required_funds_conservative(execution_price, cand, fee_rate)
        if req > available_gusd:
            break

        crypto_amount = cand

    # Final safety clamp: if even current amount doesn't fit conservative requirement, tick down.
    while crypto_amount >= min_quantity:
        req = _required_funds_conservative(execution_price, crypto_amount, fee_rate)
        if req <= available_gusd:
            break
        crypto_amount = (crypto_amount - step).quantize(step, rounding=ROUND_DOWN)

    if crypto_amount < min_quantity:
        return {
            "skipped": True,
            "reason": f"{asset}: cannot fit within available after holds/rounding",
            "effective_cap": str(effective_cap),
            "available_gusd": str(available_gusd),
        }

    # Log using your usual floor model
    order_cost, order_fee, total_cost = _compute_costs_floor(execution_price, crypto_amount, fee_rate)
    required_conservative = _required_funds_conservative(execution_price, crypto_amount, fee_rate)

    order_payload = {
        "symbol": symbol,
        "amount": str(crypto_amount),
        "price": str(execution_price),
        "side": "buy",
        "type": "exchange limit",
        "options": ["maker-or-cancel"],
    }

    return {
        "asset": asset,
        "gross_cap": str(gross_cap),
        "effective_cap": str(effective_cap),
        "available_gusd": str(available_gusd),
        "execution_price": str(execution_price),
        "crypto_amount": str(crypto_amount),
        "order_cost": str(order_cost),
        "order_fee": str(order_fee),
        "total_cost": str(total_cost),
        "required_conservative": str(required_conservative),
        "order_payload": order_payload,
    }


def place_planned_order(*, gemini: GeminiClient, plan: dict) -> dict:
    if plan.get("skipped"):
        return plan
    if "order_payload" not in plan:
        return {"error": "Invalid plan", "plan": plan}

    try:
        result = gemini.place_order(plan["order_payload"])
        return {"placed": True, "plan": plan, "result": result}
    except requests.exceptions.HTTPError as e:
        detail = ""
        try:
            detail = e.response.text if e.response is not None else ""
        except Exception:
            detail = ""
        msg = f"{str(e)}" + (f" | response: {detail}" if detail else "")
        return {"error": msg, "plan": plan}
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
            fee_rate = (Decimal(maker_bps) / Decimal("10000")).quantize(Decimal("0.00000001"), rounding=ROUND_DOWN)
            logger.info(f"Dynamic Maker Fee Rate: {fee_rate} ({maker_bps} bps)")
        except Exception as e:
            msg = f"Failed to fetch fee tier: {str(e)}. Using default 0.20%."
            logger.warning(msg)
            send_alert("Fee Rate Fetch Warning", msg)
            maker_bps = 20
            fee_rate = Decimal("0.0020")

        # Check overall balance once (coarse guard)
        gusd_before = _get_gusd_available(gemini).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
        logger.info(f"GUSD available before orders: ${gusd_before}")
        logger.info(f"MAX_BUY: ${MAX_BUY} (BTC ${BTC_AMOUNT}, ETH ${ETH_AMOUNT})")

        if gusd_before < MAX_BUY:
            error_message = f"Insufficient GUSD: ${gusd_before} < ${MAX_BUY} required."
            logger.error(error_message)
            send_alert("Crypto Buy Failed - Insufficient Funds", error_message)
            return {"statusCode": 400, "body": json.dumps({"error": error_message})}

        results = {}

        # ----------------------------
        # BTC FIRST
        # ----------------------------
        gusd_for_btc = _get_gusd_available(gemini).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
        btc_plan = plan_order_to_cap(
            gemini=gemini,
            asset="BTC",
            gross_cap=BTC_AMOUNT,
            available_gusd=gusd_for_btc,
            fee_rate=fee_rate,
            **ASSET_DEFAULTS["BTC"],
        )
        results["BTC"] = place_planned_order(gemini=gemini, plan=btc_plan)

        # ----------------------------
        # ETH SECOND (uses REAL available after BTC hold)
        # ----------------------------
        gusd_after_btc = _get_gusd_available(gemini).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
        logger.info(f"GUSD available after BTC order: ${gusd_after_btc}")

        eth_plan = plan_order_to_cap(
            gemini=gemini,
            asset="ETH",
            gross_cap=ETH_AMOUNT,
            available_gusd=gusd_after_btc,  # IMPORTANT: true remaining availability
            fee_rate=fee_rate,
            **ASSET_DEFAULTS["ETH"],
        )
        results["ETH"] = place_planned_order(gemini=gemini, plan=eth_plan)

        if any(isinstance(v, dict) and "error" in v for v in results.values()):
            send_alert("Crypto Buy Completed With Errors", json.dumps(results, indent=2))

        response_body = {
            "fee_rate_bps": maker_bps,
            "fee_rate": str(fee_rate),
            "max_buy": str(MAX_BUY),
            "btc_target": str(BTC_AMOUNT),
            "eth_target": str(ETH_AMOUNT),
            "gusd_before": str(gusd_before),
            "gusd_after_btc": str(gusd_after_btc),
            "results": results,
        }

        return {"statusCode": 200, "body": json.dumps(response_body)}

    except Exception as e:
        logger.error(f"Lambda execution failed: {str(e)}")
        send_alert("Crypto Buy Lambda Failed", str(e))
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}