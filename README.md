# Dataframe SQL Benchmark

Queries from [link](https://github.com/dragansah/tpch-dbgen/tree/master/queries)

Setup from: https://github.com/Data-Science-Platform/tpch-pgsql

## Tests

Located in [queries/tests](queries/tests). Can be run with the following command:

```python
conda activate sql_benchmark
pytest where_general_tests.py -s
pytest like_operator_tests.py -s
```

