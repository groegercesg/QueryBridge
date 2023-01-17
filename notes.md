# Parallel Sequential Scan

PostgreSQL uses multiple workers and then aggregates them
This is initialised to 2 (at maximum)
Should we control this number?

# EXPLAIN

[Documentation](https://www.postgresql.org/docs/current/sql-explain.html)

Does explain include the line number for presentation: select ___ as revenue from linenumbers

## DuckDB Fiddling

-- read a CSV file into a table

CREATE TABLE lineitem(FlightDate DATE, UniqueCarrier VARCHAR, OriginCityName VARCHAR, DestCityName VARCHAR);
COPY lineitem FROM 'test.csv' (AUTO_DETECT TRUE);

-- Change profiling settings, output in JSON and to disk

PRAGMA enable_profiling='json';
PRAGMA profile_output='/path/to/file.json';

-- Explain analyse
EXPLAIN ANALYZE Query 6;
