def moving_averages(samples, width):
    if width <= 0:
        return []
    return [
        sum(samples[start:start + width]) / width
        for start in range(len(samples) - width + 1)
    ]