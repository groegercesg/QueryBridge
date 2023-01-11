# Dataframe SQL Benchmark

## Current Comparison with SQL

![Comparison Table, Scaling Factor 1](benchmarking/analysis_results/all_queries_compare_queries.svg)

## Getting DBGEN

```bash
git clone https://github.com/edin-dal/tpch-dbgen
cp tpch-dbgen/makefile .
```

## Make the database

INSTALL IT
Run this in psql

```bash
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO benchmarker;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO benchmarker;


ALTER DEFAULT PRIVILEGES FOR USER benchmarker IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO benchmarker;
```


## Tests for sql_to_pandas

Located in [sql_to_pandas/tests](sql_to_pandas/tests). Can be run with the following command:

```bash
conda activate sql_benchmark
cd sql_to_pandas/tests
python3 -m pytest
```

