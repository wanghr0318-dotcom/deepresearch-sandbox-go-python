MILLIVOLTS_PER_VOLT = 1000


def as_volts(reading):
    """Accept numeric readings in V or mV; unknown units are invalid."""
    v, unit = reading["value"], reading["unit"]
    if unit == "V":
        return v
    if unit == "mV":
        return v / MILLIVOLTS_PER_VOLT
    raise ValueError(unit)
