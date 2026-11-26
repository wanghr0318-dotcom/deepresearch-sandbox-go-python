from decimal import Decimal


def invoice_total(amounts):
    total = sum((Decimal(amount) for amount in amounts), Decimal("0"))
    return str(total.quantize(Decimal("0.01"), rounding=ROUND_HALF_AWAY_FROM_ZERO))