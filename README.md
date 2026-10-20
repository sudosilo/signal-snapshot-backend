Signal Snapshot Backend

Signal Snapshot Backend is the service behind the signal snapshot panel. It only observes. It places no orders and talks to no brokerage.

It takes in price bars from the ticker tracker and checks them for candlestick patterns and indicators. When a rule fires it logs a snapshot of what was known at that moment. Later it grades each snapshot as a win, a loss, or a timeout, and a report shows how each rule has done. It has a health check route.

The repo is checked every morning by Control Troll.

Latest change: Created the signal snapshot backend. It takes in price bars, detects patterns, logs snapshots, grades them, and serves a report. A small deploy fix followed the same day.

See CHANGELOG.md for the full history.
