# Rolling averages omit the final complete window

Fix `moving_averages` in `records.py`. For positive `width`, return the
arithmetic mean of every contiguous complete window, including the final one.
Return an empty list if the input has no complete window.
