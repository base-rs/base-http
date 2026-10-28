# Buffered routing screen

The five-round comparison against copy-on-write used 0.30757 versus0.30695 user microseconds/request, effectively the same median. Buffered routing avoids cloning the entire routing map under overlapping mutation. It was a useful safer control, superseded by direct pointer dispatch rather than retained as additional state.
