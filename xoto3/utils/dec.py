import typing as ty
import decimal

decimal_context = decimal.Context(
    Emin=-128, Emax=126, prec=38, traps=[decimal.Clamped, decimal.Overflow, decimal.Underflow]
)


def float_to_decimal(Float: float) -> decimal.Decimal:
    return decimal_context.create_decimal(Float)


def decimal_to_number(dec: decimal.Decimal) -> ty.Union[int, float]:
    if dec % 1 == 0:
        return int(dec)
    return float(dec)
