# Installation

First check your computer has the required software:
- Python 3.8 (For SDQLpy, should be >= 3.8.10)
- Python 3.10 (For this tool, should be exactly >= 3.10.9)

Then download this project

```bash
git clone https://github.com/amirsh/dataframe-sql-benchmark
cd dataframe-sql-benchmark
```

## Getting DBGEN

First, change to the root of the project directory.

Grab the DBgen version, clone this inside the `dataframe-sql-benchmark` directory.

```bash
git clone https://github.com/edin-dal/tpch-dbgen
cp tpch-dbgen/makefile .
```

Also, get gcc and make if you don't already have these

```bash
sudo apt-get install -y make gcc
```

## Setting up Postgres

### Setting up the connection file for Postgres

The project comes included with a connection file: _database\_connection.json_

Here is the contents for it below, the Host and Port are the defaults:

```json
{
    "User": "benchmarker",
    "Password": "benchMugPassword",
    "Host": "localhost",
    "Port": "5432",
    "Database": "tpchdb"
}
```

You can change the Username, Password and Database name as you wish, but you have to use them below when setting up the databsae.

### Installing Postgres and creating the Database

Install Postgres-14 (instructions for Ubuntu 22.04):

```bash
sudo apt update
sudo apt install postgresql postgresql-contrib -y
```

<details>
<summary>Detailed Fedora 38 Install Instructions</summary>
<br>
First, add the PostgreSQL Yum Repository to your Fedora system by running the below command:

```bash
sudo dnf -y install https://download.postgresql.org/pub/repos/yum/reporpms/F-38-x86_64/pgdg-fedora-repo-latest.noarch.rpm
```

Next, install the PostgreSQL-14 Client and Server:

```bash
sudo dnf module reset postgresql -y
sudo dnf install vim postgresql14-server postgresql14
```

Then, initialize the DBMS and start the Database service:
```bash
sudo /usr/pgsql-14/bin/postgresql-14-setup initdb
sudo systemctl enable --now postgresql-14
```

And to double check everything is working, check the service status to confirm the Database came up:
```bash
systemctl status postgresql-14
```
</details>

Change to the newly created postgres user:

```bash
sudo -i -u postgres
```

And check it's installed with:

```bash
psql -c 'SELECT version();'
```

If this version is not Postgres 14.X, please install Postgres 14.

Now we can create the table, use the same name as specified in your _database\_connection.json_ file. First enter the postgres shell, then create the database.

```bash
psql
CREATE DATABASE tpchdb;
```

Next, create the database user for our program (using the same username and password as the connection file) and grant it all permissions:

```bash
CREATE USER benchmarker WITH ENCRYPTED PASSWORD 'benchMugPassword';
GRANT ALL PRIVILEGES ON DATABASE tpchdb TO benchmarker;
```

<details>
<summary>Are you trying to use your newly created user on an existing table, run these commands</summary>


Assuming your user is: _benchmarker_.

```bash
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO benchmarker;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO benchmarker;


ALTER DEFAULT PRIVILEGES FOR USER benchmarker IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO benchmarker;
```
</details>

### Disable things in Postgres that we don't support

We need to disable some things in the Postgres planner that we don't have support for. To do this, first in the Postgres shell we can find the location of the config file. And then we can edit it:
```bash
SHOW config_file;
exit
vim /var/lib/pgsql/14/data/postgresql.conf
```

We don't have support for parallelisation, bitmap scans, memoize or nested loops, so we turn these off by changing the following lines, take out the comment at the beginning. If you don't have some of these options that's okay:

```bash
max_parallel_workers_per_gather = 0
max_parallel_maintenance_workers = 0
max_parallel_workers = 0
enable_bitmapscan = off
enable_memoize = off
enable_nestloop = off
```

Then restart the postgres server with the following commands, change the postgres service name as necessary. To find the postgresql "service", please run the below command:

```bash
systemctl --type=service | grep "postgresql"
```

```bash
systemctl restart postgresql-14
systemctl status postgresql-14
```

The second command should inform whether the database has come back up.

## Setting up DuckDB

As DuckDB is an embedded DBMS, installation is made super easy. It comes with the python environment. So just follow the steps below to setup Python.

## Setting up HyperDB

Same as above

## Setting up SDQL.py

This project requires a modified version of SDQLpy, so download it from here: [OneDrive](https://uoe-my.sharepoint.com/:u:/g/personal/s1925856_ed_ac_uk/EdsZ-90C_wpNkxkZramNvW8BINuHX8i0W11y1DyknW3TKA?e=73PjAt)

Unzip it, rename the overall folder to `sdqlpy`, place it in a folder called `SDQL` and place that at the same level as `dataframe-sql-benchmark`. Here is a diagram of the desired folder structure:

```bash
.
└── Parent Folder
    ├── dataframe-sql-benchmark/
    │   └── ...
    └── SDQL/
        └── sdqlpy/
            ├── README.md
            ├── src/
            │   └── ...
            └── test/
                └── ...
```

For running SDQL.py, this project also requires Python 3.8, set at the `python3.8` alias, check this by running:

```bash
python3.8 --version
```

## Setting up Python

This project requires Python 3.10 for running the Converter and Benchmarker. This must be configured with the `python3.10` alias exactly, check this by running:

```bash
python3.10 --version
```

### Setup the Python Environment

Back in the root directory of this project, create the virtual environment and source it.
Then install the packages required.

```bash
python3.10 -m venv sqlconv
source sqlconv/bin/activate
python3.10 -m pip install -r requirements.txt
```

And the second command activates it for us.

## Using the benchmarker

The benchmarker used Test Specification files, open these and configure all options before running. For example, we will use the `hyper_duck_sdqlpy_pandas_tpch_opt_all_comparison.json` file. This runs HyperDB, DuckDB, SDQLpy (And all optimisations) and Pandas. It compares their correctness and output the results into the location specified in the Test Specification file:

```bash
conda deactivate
source sqlconv/bin/activate
python3.10 benchmarking/run_benchmarking.py --file benchmarking/test_specifications/hyper_duck_sdqlpy_pandas_tpch_opt_all_comparison.json --verbose
```
