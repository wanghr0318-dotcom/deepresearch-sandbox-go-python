# Sensor readings are added without converting units

Read `records.py` and the unit conversion contract in `units.py`. Fix
`sensor_totals` in `records.py` to return each sensor's total in volts,
summing negative and positive readings. Preserve rejection of unsupported
units as a `ValueError`. Leave the conversion helper unchanged.
