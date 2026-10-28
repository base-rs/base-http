# Direct read admission screen

Rejected. Bypassing construction/polling of the general cancellable helper for worker-owned Cancellation::never() reads measured0.290915 versus0.292472 user microseconds/request (0.53% lower median) in five interleaved five-second samples. Paired deltas varied in sign. This does not justify duplicating admission logic in the bound reader. Original reader source was restored before timing the frozen candidate and remains retained. Zero errors; no attribution of remaining overhead is claimed from this inconclusive difference.
