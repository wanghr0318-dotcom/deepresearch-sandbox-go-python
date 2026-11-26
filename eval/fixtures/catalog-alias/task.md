# Inventory summary misclassifies product labels

A daily import sometimes contains labels such as `" TEA "` or `"Pens"`.
They should aggregate into the same groups as `"tea"` and `"pens"`. Read
`records.py` and the catalogue contract in `catalog.py`, then update
`records.py`. Preserve the catalogue mapping, fallback group, and quantities.
