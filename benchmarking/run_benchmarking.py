import argparse
import sys
import json
import subprocess
from pathlib import Path
import shutil
import pandas as pd
import time
from math import log10, floor

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

def main():
    parser = init_argparse()
    args = parser.parse_args()
    
    if len(sys.argv)==1:
        # display help message when no args are passed.
        parser.print_help()
        sys.exit(1)
        
    manifest = args.file
    manifest_json = json.load(open(manifest))
    
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
    temp_path = Path(manifest_json["Temporary Directory"])
    if temp_path.exists() and temp_path.is_dir():
        shutil.rmtree(temp_path)
    # Make a folder
    Path(temp_path).mkdir(parents=True, exist_ok=True)
    
    # Write an __init__.py into the file
    open(f"{temp_path}"+"/"+"__init__.py", 'a').close()
    
    # Iterate through all the SQL Queries
    for sql_query in manifest_json["SQL Queries"]:
        print("Doing Query: " + str(sql_query["Query Name"]))
    
        # Run converter
        cmd = ["python3", manifest_json["SQL Converter Location"], '--file', sql_query["Query Location"], '--benchmarking', "True", "--output_location", manifest_json["Temporary Directory"], "--name", sql_query["Pandas Name"]]    
        
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        
        if result.returncode != 0:
            print(result.stdout)
            print(result.stderr)
            raise Exception( f'Invalid result: { result.returncode }' )      
        
        function_default = "query"
        package_name = str(sql_query["Pandas Name"]).split(".")[0]
        query_function = getattr(__import__(manifest_json["Temporary Directory"]+".%s" % package_name, fromlist=[function_default]), function_default)
        
        # Get Query Data
        query_data = []
        for relation in sql_query["Required Data"]:
            query_data.append(getattr(data_loaded, relation))
        
        run_times = []
        for i in range(manifest_json["Number of Query Runs"]):
            if args.verbose:
                print("Doing Run: " + str(i+1))
            start_time = time.time()
            result = query_function(*query_data)
            run_times.append(time.time() - start_time)
         
        if args.verbose:   
            print(run_times)
        print(round_sig(sum(run_times)/len(run_times), 3))
    
    # Run the query and get an execution time for it
    
    # Tear Down
    # Delete temporary folder
    if temp_path.exists() and temp_path.is_dir():
        shutil.rmtree(temp_path)
        
if __name__ == "__main__":
    main()
