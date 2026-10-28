# Decision

Rejected the direct helper extraction, both outlined and forced inline. The five-round inline cohort measured 0.3305 versus 0.3196 user microseconds/request for the simpler earlier ready-write implementation; three of five paired rounds were worse. Throughput had substantial interference/placement variation, including one 108k outlier, and establishes no improvement. The simpler ready-write implementation is restored.
