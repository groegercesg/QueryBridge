# Parallel Sequential Scan

PostgreSQL uses multiple workers and then aggregates them
This is initialised to 2 (at maximum)
Should we control this number?

# EXPLAIN

[Documentation](https://www.postgresql.org/docs/current/sql-explain.html)

Does explain include the line number for presentation: select ___ as revenue from linenumbers