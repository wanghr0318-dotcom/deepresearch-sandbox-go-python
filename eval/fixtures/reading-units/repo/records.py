from units import as_volts


def sensor_totals(readings):
    totals = {}
    for reading in readings:
        name = reading["sensor"]
        totals[name] = totals.get(name, 0) + reading["value"]
    return totals
