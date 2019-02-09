# Simple DB

A key-value store that is:
- single-table
- concurrent
- buffered in-memory
- really bad about write amplification
- simple
- interesting to reason about

The design is intended for ease of verification while still retaining many hard concurrency and crash safety challenges.
