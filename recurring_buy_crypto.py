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
MAX_BUY = (TOTAL_DEPOSIT / 2).quantize(Decimal("0.01"), rounding=ROUND_DOWN)

BTC_PERCENTAGE = Decimal("66")
ETH_PERCENTAGE = Decimal("34")

# Target gross allocations (for reporting / intent). Actual spend may end up 1–2 cents under.
BTC_TARGET = (MAX_BUY * (BTC_PERCENTAGE / Decimal("100"))).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
ETH_TARGET = (MAX_BUY - BTC_TARGET).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

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
    # Gemini quote currency is cents; keep that consistent.
    return (ask * slippage_factor).quantize(Decimal("0.01"), rounding=ROUND_DOWN)


def _compute_cost_floor(execution_price: Decimal, crypto_amount: Decimal) -> Decimal:
    return (execution_price * crypto_amount).quantize(Decimal("0.01"), rounding=ROUND_DOWN)


def _compute_fee_floor(cost: Decimal, fee_rate: Decimal) -> Decimal:
    return (cost * fee_rate).quantize(Decimal("0.01"), rounding=ROUND_DOWN)


def _compute_fee_ceil(cost: Decimal, fee_rate: Decimal) -> Decimal:
    return (cost * fee_rate).quantize(Decimal("0.01"), rounding=ROUND_UP)


def _required_funds_conservative(execution_price: Decimal, crypto_amount: Decimal, fee_rate: Decimal) -> Decimal:
    """
    Conservative funds check that matches why Gemini rejects orders:
      cost: floor to cents
      fee:  ceil to cents
      required = cost + fee
    """
    cost = _compute_cost_floor(execution_price, crypto_amount)
    fee = _compute_fee_ceil(cost, fee_rate)
    return (cost + fee).quantize(Decimal("0.01"), rounding=ROUND_UP)


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
    Plans a maker-or-cancel limit order sized to:
      - NEVER require more than gross_cap under the conservative requirement model.
      - Spend as close to gross_cap as possible (max out by tick increments).
    """
    gross_cap = gross_cap.quantize(Decimal("0.01"), rounding=ROUND_DOWN)

    if gross_cap <= Decimal("0.00"):
        return {"skipped": True, "reason": "gross_cap <= 0", "gross_cap": str(gross_cap)}

    execution_price = _get_execution_price(gemini, symbol, slippage_factor)
    step = _quant_step(tick_size)

    # Start from a safe under-estimate.
    # (gross / (price*(1+fee))) is a decent lower bound; final acceptance uses conservative check.
    denom = execution_price * (Decimal("1") + fee_rate)
    if denom <= Decimal("0"):
        return {"skipped": True, "reason": "bad denom", "execution_price": str(execution_price)}

    crypto_amount = (gross_cap / denom).quantize(step, rounding=ROUND_DOWN)

    # Ensure minimum
    if crypto_amount < min_quantity:
        return {
            "skipped": True,
            "reason": f"{asset}: below min quantity",
            "gross_cap": str(gross_cap),
            "execution_price": str(execution_price),
            "crypto_amount": str(crypto_amount),
            "min_quantity": str(min_quantity),
        }

    # Bump by one tick while the CONSERVATIVE required funds still fits in cap.
    while True:
        cand = (crypto_amount + step).quantize(step, rounding=ROUND_DOWN)
        req = _required_funds_conservative(execution_price, cand, fee_rate)
        if req > gross_cap:
            break
        crypto_amount = cand

    # Final requirement + reporting fields
    order_cost = _compute_cost_floor(execution_price, crypto_amount)
    order_fee_floor = _compute_fee_floor(order_cost, fee_rate)
    total_cost_floor = (order_cost + order_fee_floor).quantize(Decimal("0.01"), rounding=ROUND_DOWN)

    required_conservative = _required_funds_conservative(execution_price, crypto_amount, fee_rate)
    fee_conservative = _compute_fee_ceil(order_cost, fee_rate)

    if required_conservative > gross_cap:
        # Very defensive; should not happen due to loop.
        return {
            "skipped": True,
            "reason": f"{asset}: cannot fit within cap under conservative hold",
            "gross_cap": str(gross_cap),
            "required_conservative": str(required_conservative),
        }

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
        "execution_price": str(execution_price),
        "crypto_amount": str(crypto_amount),
        "order_cost": str(order_cost),
        "order_fee_floor": str(order_fee_floor),
        "total_cost_floor": str(total_cost_floor),
        "fee_conservative": str(fee_conservative),
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

        # 1) Fetch maker fee tier dynamically
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

        # 2) Coarse guard: must have at least MAX_BUY available to attempt the run
        gusd_before = _get_gusd_available(gemini).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
        logger.info(f"GUSD available before orders: ${gusd_before}")
        logger.info(f"MAX_BUY: ${MAX_BUY} | Targets BTC ${BTC_TARGET} / ETH ${ETH_TARGET}")

        if gusd_before < MAX_BUY:
            error_message = f"Insufficient GUSD: ${gusd_before} < ${MAX_BUY} required."
            logger.error(error_message)
            send_alert("Crypto Buy Failed - Insufficient Funds", error_message)
            return {"statusCode": 400, "body": json.dumps({"error": error_message})}

        # 3) Plan BTC to BTC_TARGET (never exceed under conservative hold model)
        btc_plan = plan_order_to_cap(
            gemini=gemini,
            asset="BTC",
            gross_cap=BTC_TARGET,
            fee_rate=fee_rate,
            **ASSET_DEFAULTS["BTC"],
        )

        # Determine remaining budget based on conservative requirement (NOT on balances)
        if btc_plan.get("skipped"):
            btc_required = Decimal("0.00")
        else:
            btc_required = Decimal(str(btc_plan["required_conservative"]))

        remaining_budget = (MAX_BUY - btc_required).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
        if remaining_budget < Decimal("0.00"):
            remaining_budget = Decimal("0.00")

        # 4) Plan ETH as remainder, capped by target and remaining budget
        eth_cap = min(ETH_TARGET, remaining_budget).quantize(Decimal("0.01"), rounding=ROUND_DOWN)

        if eth_cap <= Decimal("0.00"):
            eth_plan = {
                "skipped": True,
                "reason": "No remaining budget after BTC conservative hold",
                "remaining_budget": str(remaining_budget),
                "eth_target": str(ETH_TARGET),
            }
            eth_required = Decimal("0.00")
        else:
            eth_plan = plan_order_to_cap(
                gemini=gemini,
                asset="ETH",
                gross_cap=eth_cap,
                fee_rate=fee_rate,
                **ASSET_DEFAULTS["ETH"],
            )
            eth_required = Decimal("0.00") if eth_plan.get("skipped") else Decimal(str(eth_plan["required_conservative"]))

        # 5) Final sanity: conservative required totals must be <= MAX_BUY
        total_required = (btc_required + eth_required).quantize(Decimal("0.01"), rounding=ROUND_UP)
        if total_required > MAX_BUY:
            # This should be impossible now, but keep a guard.
            msg = f"Internal safety stop: required {total_required} > MAX_BUY {MAX_BUY}"
            logger.error(msg)
            send_alert("Crypto Buy Failed - Safety Stop", msg)
            return {"statusCode": 500, "body": json.dumps({"error": msg})}

        # 6) Place orders (BTC first, then ETH)
        results = {}
        results["BTC"] = place_planned_order(gemini=gemini, plan=btc_plan)
        results["ETH"] = place_planned_order(gemini=gemini, plan=eth_plan)

        if any(isinstance(v, dict) and "error" in v for v in results.values()):
            send_alert("Crypto Buy Completed With Errors", json.dumps(results, indent=2))

        body = {
            "fee_rate_bps": maker_bps,
            "fee_rate": str(fee_rate),
            "max_buy": str(MAX_BUY),
            "gusd_before": str(gusd_before),
            "targets": {"btc": str(BTC_TARGET), "eth": str(ETH_TARGET)},
            "planned_caps": {"btc_cap": str(BTC_TARGET), "eth_cap": str(eth_cap)},
            "required_conservative": {"btc": str(btc_required), "eth": str(eth_required), "total": str(total_required)},
            "results": results,
        }

        return {"statusCode": 200, "body": json.dumps(body)}

    except Exception as e:
        logger.error(f"Lambda execution failed: {str(e)}")
        send_alert("Crypto Buy Lambda Failed", str(e))
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}