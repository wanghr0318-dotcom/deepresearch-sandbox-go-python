# Invoice total rounds half cents the wrong way

`invoice_total(["1.00", "0.005"])` should return the string `"1.01"`.
Repair `records.py`. Sum the exact decimal amounts first, then round to two places
with ties away from zero. Keep the decimal strings and the two-place output.
