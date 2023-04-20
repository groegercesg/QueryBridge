from generate_tpch_data import data_generator 
import importlib.util
import sys
import os
import argparse

class HiddenPrinting:
    def __enter__(self):
        self._original_stdout = sys.stdout
        sys.stdout = open(os.devnull, 'w')

    def __exit__(self, exc_type, exc_val, exc_tb):
        sys.stdout.close()
        sys.stdout = self._original_stdout

import prepare_postgres
import prepare_duckdb

def do_postgres(verbose, data_storage_location, postgres_connection_details, constants_location):
    # Prepare Postgres
        # Loads the data from "Data Storage" and puts it into the database
    print("Preparing Postgres Database")
    pg_db = prepare_postgres.prep_pg(postgres_connection_details)
    if verbose:
        pg_db.prepare_database(data_storage_location, constants_location)
    else:
        with HiddenPrinting():
            pg_db.prepare_database(data_storage_location, constants_location)
    print("Postgres Database prepared")

def do_duckdb(verbose, data_storage_location, duck_db_connection_details):
    # Prepare DuckDB
        # Loads the data from "Data Storage" and puts it into the DuckDB file
    print("Preparing DuckDB Database")
    duck_db = prepare_duckdb.prep_duck(duck_db_connection_details)
    if verbose:
        duck_db.prepare_database(data_storage_location)
    else:
        with HiddenPrinting():
            duck_db.prepare_database(data_storage_location)
    print("DuckDB Database prepared")

def prepare_all(verbose, data_storage_location, db_gen_location, scaling_factor, postgres_connection_details,
                duck_db_connection_details, constants_location, run_only = None):
    # Data generator
        # Generates the data and saves it to:
        # "Data Storage"
    print("Preparing the TPC-H Data")
    if verbose:
        data_generator(data_storage_location, db_gen_location, scaling_factor=float(scaling_factor))
    else:
        with HiddenPrinting():
            data_generator(data_storage_location, db_gen_location, scaling_factor=float(scaling_factor))
    print("TPC-H Data Prepared")
    
    if (run_only == None):
        # Do Postgres
        do_postgres(verbose, data_storage_location, postgres_connection_details, constants_location)
        # Do Duck DB
        do_duckdb(verbose, data_storage_location, duck_db_connection_details)

    elif (run_only != None) and (run_only == "PostgreSQL"):
        do_postgres(verbose, data_storage_location, postgres_connection_details, constants_location)
    elif (run_only != None) and (run_only == "DuckDB"):
        do_duckdb(verbose, data_storage_location, duck_db_connection_details)
    else:
        raise Exception("Unexpected options for '--run_only. The supported options are: \n\t'PostgreSQL'\n\t'DuckDB'")

    
def str2bool(v):
    if isinstance(v, bool):
        return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')    

def init_argparse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        usage="%(prog)s [OPTION] [FILE]...",
        description="Prepare the Database"
    )
    parser.add_argument(
        "-v", "--version", action="version",
        version = f"{parser.prog} version 0.0.1"
    )
    
    requiredNamed = parser.add_argument_group('required named arguments')
    
    requiredNamed.add_argument('--verbose',
                        metavar='verbose',
                        type=str2bool,
                        help='Whether to produce noisy progress output',
                        required=True)
    requiredNamed.add_argument('--data_storage',
                        metavar='data_storage',
                        type=str,
                        help='The location where the preparation tool should store the data',
                        required=True)
    requiredNamed.add_argument('--db_gen',
                        metavar='db_gen',
                        type=str,
                        help='The location of db gen',
                        required=True)
    requiredNamed.add_argument('--scaling_factor',
                        metavar='scaling_factor',
                        type=int,
                        help='The scaling factor for our data',
                        required=True)
    requiredNamed.add_argument('--postgres_connection',
                        metavar='postgres_connection',
                        type=str,
                        help='The file that contains the postgres connection',
                        required=True)
    requiredNamed.add_argument('--duck_db_connection',
                        metavar='duck_db_connection',
                        type=str,
                        help='The file that contains the duck_db connection',
                        required=True)   
    requiredNamed.add_argument('--constants',
                        metavar='constants',
                        type=str,
                        help='The location where information like prep_queries are stored',
                        required=True)
    requiredNamed.add_argument('--run_only',
                        metavar='run_only',
                        type=str,
                        help='Run only a certain database',
                        default=None
                        )
    
    return parser

if __name__ == "__main__":
    
    parser = init_argparse()
    args = parser.parse_args()
    
    if len(sys.argv)==1:
        # display help message when no args are passed.
        parser.print_help()
        sys.exit(1)
    
    prepare_all(args.verbose, args.data_storage, args.db_gen, args.scaling_factor, args.postgres_connection, args.duck_db_connection, args.constants, args.run_only)
