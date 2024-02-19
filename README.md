# Dataframe SQL Benchmark

## Installation

First check your computer has the required software:
- Python 3.8 (For SDQLpy, should be >= 3.8.10)
- Python 3.10 (For this tool, should be exactly >= 3.10.13)

Then download this project

```bash
git clone https://github.com/amirsh/dataframe-sql-benchmark
cd dataframe-sql-benchmark
```

### Getting DBGEN

First, change to the root of the project directory.

Grab the DBgen version, clone this into the project directory.

```bash
git clone https://github.com/edin-dal/tpch-dbgen
cp tpch-dbgen/makefile .
```

Also, get gcc and make if you don't already have these

```bash
sudo apt-get install -y make gcc
```

### Setting up DuckDB

As DuckDB is an embedded DBMS, installation is made super easy. It comes with the python environment. So just follow the steps below to setup Python.

### Setting up HyperDB

Same as above

### Setting up SDQL.py

This project requires a modified version of SDQLpy, so download it from here: [OneDrive](https://uoe-my.sharepoint.com/:u:/g/personal/s1925856_ed_ac_uk/EdsZ-90C_wpNkxkZramNvW8BINuHX8i0W11y1DyknW3TKA?e=73PjAt)

Unzip it, rename the overall folder to `sdqlpy`, place it in a folder called `SDQLPY` and place that at the same level as `dataframe-sql-benchmark`. Here is a diagram of the desired folder structure:

```bash
.
└── Parent/
    ├── dataframe-sql-benchmark/
    │   └── ...
    └── SDQLPY/
        └── sdqlpy/
            ├── README.md
            ├── src/
            │   └── ...
            └── test/
                └── ...
```

### Setting up Python

This project requires python version 3.10 exactly, check this by running:

```bash
python --version
```

#### Setup the Python Environment

Back in the root directory of this project, create the virtual environment and source it.
Then install the packages required.

```bash
python3 -m venv sqlconv_env
source sqlconv_env/bin/activate
pip install -r requirements.txt
```

And the second command activates it for us.

## Using the benchmarker

The benchmarker used Test Specification files, open these and configure all options before running. For example, we will use the `hyper_duck_sdqlpy_pandas_tpch_opt_all_comparison.json` file. This runs HyperDB, DuckDB, SDQLpy (And all optimisations) and Pandas. It compares their correctness and output the results into the desired file:

```bash
conda deactivate
source sqlconv/env/activate
python3.10 benchmarking/run_benchmarking.py --file benchmarking/test_specifications/hyper_duck_sdqlpy_pandas_tpch_opt_all_comparison.json --verbose
```
