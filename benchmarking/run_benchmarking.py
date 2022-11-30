import argparse
import sys
import json
import subprocess
from pathlib import Path
import shutil
import time
import csv
from math import log10, floor
from query_database import run_query
from compare_results import compare
import os

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
        description="Benchmark SQL and Pandas queries, based on instructions from the manifest file."
    )
    parser.add_argument(
        "-v", "--version", action="version",
        version = f"{parser.prog} version 0.0.1"
    )
    parser.add_argument('--file',
                       metavar='file',
                       type=str,
                       help='The file contain instructions for our testing')
    
    parser.add_argument('--verbose',
                       metavar='verbose',
                       type=str2bool,
                       nargs='?',
                       const=True, 
                       default=False,
                       help='Whether the benchmark script makes lots of output or not a lot')
    
    return parser

def round_sig(x, sig=3):
    return round(x, sig-int(floor(log10(abs(x))))-1)

def results_writer(json, data):
    with open(json["Results Location"], 'a', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(data)

def main():
    parser = init_argparse()
    args = parser.parse_args()
    
    if len(sys.argv)==1:
        # display help message when no args are passed.
        parser.print_help()
        sys.exit(1)
        
    manifest = args.file
    manifest_json = json.load(open(manifest))
    
    db_details = manifest_json["Database Connection Details"]
    
    print("Running Test: " + str(manifest_json['Test Name']))
    
    # Import Pandas Data
    print("Importing Pandas Data")
    data_loader = str(manifest_json["Pandas Data Loader"]).split(".")[0]
    data_loaded = __import__(data_loader)
    print("Data Imported, now Running Queries")
    
    # We will run the converter to create our pandas files from our sql files
    # We want this to output the converted queries to a given directory
    # This directory is specified as: manifest_json["Temporary Directory"]
    
    # Delete if already exists
    def make_temp_folder():
        temp_path = Path(manifest_json["Temporary Directory"])
        if temp_path.exists() and temp_path.is_dir():
            shutil.rmtree(temp_path)
        # Make a folder
        Path(temp_path).mkdir(parents=True, exist_ok=True)
        
        # Write an __init__.py into the file
        open(f"{temp_path}"+"/"+"__init__.py", 'a').close()
        
        return temp_path
        
    def delete_temp_folder():
        # Tear Down
        # Delete temporary folder
        if temp_path.exists() and temp_path.is_dir():
            shutil.rmtree(temp_path)
        
    temp_path = make_temp_folder()
    
    # Delete Results file if already exists
    if os.path.exists(manifest_json["Results Location"]):
        os.remove(manifest_json["Results Location"])
        
    # For the results, write the Header
    results_writer(manifest_json, ["Data Type", "Query Name", "Average", "Runs"])
    
    # Iterate through all the SQL Queries
    for sql_query in manifest_json["SQL Queries"]:
        temp_path = make_temp_folder()
        
        print("Doing Query: " + str(sql_query["Query Name"]))
    
        # Run converter
        cmd = ["python3", manifest_json["SQL Converter Location"], '--file', sql_query["Query Location"], '--benchmarking', "True", "--output_location", manifest_json["Temporary Directory"], "--name", sql_query["Pandas Name"]]    
        if "Conversion Options" in sql_query:
            cmd += sql_query["Conversion Options"]
        
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        
        if result.returncode != 0:
            print(result.stdout)
            print(result.stderr)
            raise Exception( f'Invalid result: { result.returncode }' )      
        
        function_default = "query"
        package_name = str(sql_query["Pandas Name"]).split(".")[0]
        query_function = getattr(__import__(manifest_json["Temporary Directory"]+".%s" % package_name, fromlist=[function_default]), function_default)
        
        # Order of data imports
        data_order = list(query_function.__code__.co_varnames[:query_function.__code__.co_argcount])
        
        # Get Query Data
        query_data = [0] * len(data_order)
        for relation in sql_query["Required Data"]:
            # Get position of relation in data_order, use as position in query data
            insert_pos = data_order.index(relation)
            query_data[insert_pos] = getattr(data_loaded, relation)
        
        pandas_run_times = []
        # Run the query and get an execution time for it
        for i in range(manifest_json["Number of Query Runs"]):
            if args.verbose:
                print("Doing Pandas Run: " + str(i+1))
            
            start_time = time.time()
            pandas_result = query_function(*query_data)
            end_time = time.time()
            
            pandas_run_times.append(end_time - start_time)
         
        if args.verbose:   
            print(pandas_run_times)
        avg_3sf = round_sig(sum(pandas_run_times)/len(pandas_run_times), 3)
        print("Pandas: " + str(avg_3sf))
        results_writer(manifest_json, [str(sql_query["Results Name"]), str(sql_query["Query Name"]), avg_3sf, pandas_run_times])
        
        sql_run_times = []
        # Handle whether to run SQL many times or just run once to check correctness
        sql_runs = None
        if "Compare SQL" in sql_query:
            if sql_query["Compare SQL"] == "Once":
                sql_runs = 1
            else:
                raise ValueError("Unknown Value for Compare SQL in Query: " + str(sql_query["Query Name"]) + ". This was: " + str(sql_query["Compare SQL"]))
        else:
            sql_runs = manifest_json["Number of Query Runs"]
        for i in range(sql_runs):
            if args.verbose:
                print("Doing SQL Run: " + str(i+1))
            sql_result, run_time = run_query(db_details, sql_query["Query Location"], args.verbose)
            sql_run_times.append(run_time)
            
        if args.verbose:   
            print(sql_run_times)
        avg_3sf = round_sig(sum(sql_run_times)/len(sql_run_times), 3)
        print("SQL: " + str(avg_3sf))
        if "Compare SQL" not in sql_query:
            # If we have set our "Compare SQL", then don't write the SQL results to file
            results_writer(manifest_json, ["SQL", str(sql_query["Query Name"]), avg_3sf, sql_run_times])
        
        # Checking correctness
        # We should check if pandas_result is the same as sql_result
        compare_decision, columns = compare(sql_query["Query Location"], pandas_result, sql_result, manifest_json["Results Precision"])
        if compare_decision:
            print("The returned data was equivalent for both SQL and Pandas")
        else:
            print("The returned data was not equivalent!")
            print("Pandas Data:")
            print(pandas_result)
            print("SQL Data:")
            print(columns)
            for row in sql_result:
                print(row)
        
        delete_temp_folder()
    
    print("Testing is complete and results have been written to: " + str(manifest_json["Results Location"]))    
    
    delete_temp_folder()
        
if __name__ == "__main__":
    main()
