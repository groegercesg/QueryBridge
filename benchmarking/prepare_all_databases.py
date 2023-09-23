from generate_tpch_data import data_generator

from prepare_databases.prepare_duckdb import PrepareDuckDB
from prepare_databases.prepare_postgres import PreparePostgres

import sys
import os
import argparse
from enum import Enum

class HiddenPrinting:
    def __enter__(self):
        self._original_stdout = sys.stdout
        sys.stdout = open(os.devnull, 'w')

    def __exit__(self, exc_type, exc_val, exc_tb):
        sys.stdout.close()
        sys.stdout = self._original_stdout
        
class KnownDatabases(Enum):
    POSTGRES = PreparePostgres
    DUCK_DB = PrepareDuckDB
        
def run_prepare(prepare_class, verbose, data_storage_location, connection_details, constants_location=None):
    if isinstance(prepare_class, KnownDatabases):
        prepare_class = prepare_class.value
    else:
        raise Exception(f"The prepare_class was of type: {type(prepare_class)}")
    
    # Prepare the passed class: prepare_class
    prep_db = prepare_class(connection_details)
    print(f"Prepare {prep_db.database_name} Database")
    if verbose:
        prep_db.prepare_database(data_storage_location, constants_location)
    else:
        with HiddenPrinting():
            prep_db.prepare_database(data_storage_location, constants_location)
    print(f"{prep_db.database_name} Database Prepared")

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
        run_prepare(KnownDatabases.POSTGRES, verbose, data_storage_location, postgres_connection_details, constants_location)
        # Do Duck DB
        run_prepare(KnownDatabases.DUCK_DB, verbose, data_storage_location, duck_db_connection_details, constants_location)

    elif (run_only != None) and (run_only == "PostgreSQL"):
        run_prepare(KnownDatabases.POSTGRES, verbose, data_storage_location, postgres_connection_details, constants_location)
    elif (run_only != None) and (run_only == "DuckDB"):
        run_prepare(KnownDatabases.DUCK_DB, verbose, data_storage_location, duck_db_connection_details, constants_location)
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
