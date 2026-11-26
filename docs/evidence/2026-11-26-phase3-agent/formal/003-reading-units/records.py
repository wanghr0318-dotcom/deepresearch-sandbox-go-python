from units import as_volts


def sensor_totals(readings):
    totals = {}
    for reading in readings:
        name = reading["sensor"]
        value_in_volts = as_volts(reading)
        totals[name] = totals.get(name, 0) + value_in_volts
    return totals