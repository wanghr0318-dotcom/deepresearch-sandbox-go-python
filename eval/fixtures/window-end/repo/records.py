def moving_averages(samples, width):
    return [
        sum(samples[start:start + width]) / width
        for start in range(len(samples) - width)
    ]
