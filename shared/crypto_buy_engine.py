import logging
from decimal import Decimal, ROUND_DOWN

from shared.gemini_client import GeminiClient

logger = logging.getLogger(__name__)

# Ticks below best bid for the initial MOC attempt
PASSIVE_TICKS_BELOW_BID = 1

# Ticks below best bid for the fallback resting order (deeper = less likely to cross)
FALLBACK_TICKS_BELOW_BID = 3

GUSD_FLOOR = Decimal("1.00")


def _quant_step(tick_size: int) -> Decimal:
    return Decimal("1").scaleb(-tick_size)


def _fetch_book(gemini: GeminiClient, symbol: str):
    book = gemini.get_book(symbol)
    best_bid = Decimal(str(book["bids"][0]["price"]))
    best_ask = Decimal(str(book["asks"][0]["price"]))
    return best_bid, best_ask


def _compute_order(gross_amount: Decimal, price: Decimal, fee_rate: Decimal, tick: Decimal):
    qty = (gross_amount / (price * (Decimal("1") + fee_rate))).quantize(tick, ROUND_DOWN)
    estimated_fee = (price * qty * fee_rate).quantize(Decimal("0.01"), ROUND_DOWN)
    estimated_total = (price * qty + estimated_fee).quantize(Decimal("0.01"), ROUND_DOWN)
    return qty, estimated_fee, estimated_total


def execute_buy(
    gemini: GeminiClient,
    asset_name: str,
    config: dict,
    maker_fee: Decimal,
    taker_fee: Decimal,
    gusd_balance: Decimal,
) -> dict:
    """
    Place a fee-inclusive limit buy order for a single asset.

    config requires:
        symbol        (str)     — e.g. "btcgusd"
        amount        (Decimal) — gross GUSD budget including fees
        tick_size     (int)     — qty decimal places
        min_quantity  (Decimal) — exchange minimum order size
        price_tick    (Decimal) — price decimal increment (e.g. Decimal("0.01"))

    Returns a result dict with keys:
        mode          — "maker" | "fallback_limit" | "skipped" | "error"
        order_id      — present if an order was placed
        ...
    """
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
    min_qty = config["min_quantity"]

    # -------------------------------------------------------
    # ATTEMPT 1: MOC at best_bid - 1 tick (guaranteed maker fee)
    # -------------------------------------------------------
    best_bid, best_ask = _fetch_book(gemini, symbol)
    logger.info(f"{asset_name}: best_bid={best_bid} best_ask={best_ask} spread={best_ask - best_bid}")

    price = max(price_tick, (best_bid - PASSIVE_TICKS_BELOW_BID * price_tick).quantize(price_tick, ROUND_DOWN))
    qty, estimated_fee, estimated_total = _compute_order(gross_amount, price, maker_fee, tick)

    logger.info(
        f"{asset_name} [MOC attempt]: price={price} qty={qty} "
        f"est_fee={estimated_fee} est_total={estimated_total} budget={gross_amount}"
    )

    if qty < min_qty:
        msg = f"{asset_name}: qty {qty} below minimum {min_qty}"
        logger.error(msg)
        return {"error": msg}

    moc_payload = {
        "symbol": symbol,
        "amount": str(qty),
        "price": str(price),
        "side": "buy",
        "type": "exchange limit",
        "options": ["maker-or-cancel"],
    }

    moc_result = gemini.place_order(moc_payload)
    order_id = moc_result.get("order_id")

    if not (moc_result.get("is_cancelled") and moc_result.get("reason") == "MakerOrCancelWouldTake"):
        # MOC accepted (order is live with guaranteed maker fee)
        logger.info(f"{asset_name}: MOC order placed, order_id={order_id}")
        return {
            "mode": "maker",
            "order_id": order_id,
            "price": str(price),
            "qty": str(qty),
            "estimated_fee": str(estimated_fee),
            "estimated_total": str(estimated_total),
            "budget": str(gross_amount),
            "result": moc_result,
        }

    # -------------------------------------------------------
    # ATTEMPT 2: Book moved. Refetch and place resting limit.
    # Priced deeper to avoid crossing. Sized with taker_fee as
    # a conservative buffer since the book is clearly moving.
    # Order will always be placed to earmark GUSD.
    # -------------------------------------------------------
    logger.warning(f"{asset_name}: MOC rejected (MakerOrCancelWouldTake) — refetching book and placing fallback")

    best_bid, best_ask = _fetch_book(gemini, symbol)
    logger.info(f"{asset_name} [fallback]: best_bid={best_bid} best_ask={best_ask} spread={best_ask - best_bid}")

    price = max(price_tick, (best_bid - FALLBACK_TICKS_BELOW_BID * price_tick).quantize(price_tick, ROUND_DOWN))
    qty, estimated_fee, estimated_total = _compute_order(gross_amount, price, taker_fee, tick)

    logger.info(
        f"{asset_name} [fallback]: price={price} qty={qty} "
        f"est_fee={estimated_fee} est_total={estimated_total} budget={gross_amount}"
    )

    if qty < min_qty:
        msg = f"{asset_name}: fallback qty {qty} below minimum {min_qty}"
        logger.error(msg)
        return {"error": msg}

    fallback_payload = {
        "symbol": symbol,
        "amount": str(qty),
        "price": str(price),
        "side": "buy",
        "type": "exchange limit",
    }

    fallback_result = gemini.place_order(fallback_payload)
    order_id = fallback_result.get("order_id")
    logger.info(f"{asset_name}: Fallback order placed, order_id={order_id}")

    return {
        "mode": "fallback_limit",
        "order_id": order_id,
        "price": str(price),
        "qty": str(qty),
        "estimated_fee": str(estimated_fee),
        "estimated_total": str(estimated_total),
        "budget": str(gross_amount),
        "result": fallback_result,
    }