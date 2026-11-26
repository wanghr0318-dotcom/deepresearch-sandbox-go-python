# Cursor pagination skips tied timestamps

Rows have integer `time` and string `id` fields. The cursor is the last seen
`[time, id]` pair. Fix `next_page` in `records.py` so pages preserve ascending
`(time, id)` order and neither skip nor repeat a row when timestamps tie.
