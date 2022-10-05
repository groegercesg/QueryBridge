# Installing PSQL

Using the following [guide](https://computingforgeeks.com/install-postgresql-database-fedora-linux/) which is specific to Fedora.

Also install `postgresql-devel, libpq5-devel` (to provide pg_config command)

## Check postgresql-14 status

``` systemctl status postgresql-14 ```[](https://github.com/dragansah/tpch-dbgen/blob/master/queries/6.sql)

## Using tpch4pgsql

- Prepare
    ```python3 tpch_pgsql.py -U tpch -W dogSocks -d tpchdb prepare```
- Load
    ```python3 tpch_pgsql.py -U tpch -W dogSocks -d tpchdb load```
- Query
    ```python3 tpch_pgsql.py -U tpch -W dogSocks -d tpchdb query```

## Running in cmd

Be in this folder:
`dataframe-sql-benchmark/queries`

Run this:
`psql -d tpchdb -U tpch -a -f 6.sql`


### Solve Peer auth issue

[link](https://stackoverflow.com/questions/18664074/getting-error-peer-authentication-failed-for-user-postgres-when-trying-to-ge)

