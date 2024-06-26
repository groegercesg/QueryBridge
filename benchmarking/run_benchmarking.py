import argparse
import sys
# Add the parent directory of package to sys.path before attempting to import anything from package using absolute imports:
from pathlib import Path # if you haven't already done so
current_file = Path(__file__).resolve()
parent, root = current_file.parent, current_file.parents[1]
sys.path.append(str(root))
import json
import subprocess
import shutil
import time
import csv
from math import log10, floor
from query_pg_database import run_pg_query
from query_duck_database import run_duck_query
from query_hyper_database import run_hyper_query
from compare_results import compare
from prepare_all_databases import prepare_all
import os
from os import path
import importlib.util
import sys
import re
import pandas as pd
import stat
from sdqlpy_running import run_sdqlpy, setup_sdqlpy, teardown_sdqlpy

class HiddenPrinting:
    def __enter__(self):
        self._original_stdout = sys.stdout
        sys.stdout = open(os.devnull, 'w')

    def __exit__(self, exc_type, exc_val, exc_tb):
        sys.stdout.close()
        sys.stdout = self._original_stdout

def validateJSON(path):
    try:
        json.load(open(path))
    except ValueError as err:
        print(err)
        return False
    return True

class color:
    PURPLE = '\033[95m'
    CYAN = '\033[96m'
    DARKCYAN = '\033[36m'
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    END = '\033[0m'

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

    # Validate JSON
    if validateJSON(manifest) != True:
        print("JSON Error: The JSON test specification file supplied has Syntax Errors")
        exit(0)

    manifest_json = json.load(open(manifest))
    
    with open(manifest_json["Postgres Connection Details"], "r") as f:
        pg_db_details = json.load(f)
        
    duck_db_details = manifest_json["Duck DB Connection"]
    hyper_db_details = manifest_json["Hyper DB Connection"]
        
    # Store Queries
    store_queries = False
    if "Store Queries" in manifest_json:
        store_queries = True
        store_queries_file_content = ""
        
        # Create variable for function call and function_body
        store_queries_function_call = ""
        store_queries_function_body = ""
        
        # Add dataloader content
        if ("Data Loader" in manifest_json["Store Queries"]):
            f = open(manifest_json["Store Queries"]["Data Loader"], "r")
            dataloader_content = str(f.read())
        
            store_queries_file_content += dataloader_content
            #store_queries_file_content += "\n\n\n"
        
    print("Running Test: " + str(manifest_json['Test Name']))
    
    # Delete Results file if already exists
    if os.path.exists(manifest_json["Results Location"]):
        os.remove(manifest_json["Results Location"])
        
    # For the results, write the Header
    results_writer(manifest_json, ["Data Type", "Scaling Factor", "Query Name", "Query Number", "Average", "Correct", "Executed", "Runs"])
    
    first_run = True
    
    # HyperThreading
    if "HyperThreading Off" in manifest_json:
        if manifest_json["HyperThreading Off"] == "True":
            os.environ["NO_HYPER_THREADING"] = "1"
        else:
            os.environ["NO_HYPER_THREADING"] = "-1"
    
    # Check we have permissions to change hyper threading modes
    if os.access("/sys/devices/system/cpu/smt/control", os.W_OK):
        pass
    else:
        raise Exception("Don't have permissions to write to '/sys/devices/system/cpu/smt/control'. Maybe you should 'chmod 777' it")
            
    # Number of threads
    number_of_threads = 1
    if "Number of Threads" in manifest_json:
        number_of_threads = int(manifest_json["Number of Threads"])
            
    # Iterate through scaling factors
    for scaling_factor in manifest_json["Scaling Factors"]:
        print("Doing Scaling Factor: " + str(scaling_factor))
        
        if "Regenerate Data" in manifest_json and manifest_json["Regenerate Data"] == "False":
            # TODO: Check that PG/DuckDB have the required data!
            # Don't regenerate the data
            print("We are skipping generating new Data!")
            pass
        else:
            # Prepare databases
            prepare_only = None
            if "Prepare Only" in manifest_json:
                prepare_only = manifest_json["Prepare Only"]
            prepare_all(args.verbose, manifest_json["Data Storage"],
                        manifest_json["DB Gen Location"], scaling_factor,
                        manifest_json["Postgres Connection Details"],
                        manifest_json["Duck DB Connection"], manifest_json["Hyper DB Connection"],
                        manifest_json["Constants Location"],
                        number_of_threads, prepare_only)
        
        # Import Pandas Data
        print("Importing Pandas Data")
        if first_run == True:
            data_loader = str(manifest_json["Pandas Data Loader"]).split(".")[0]
            data_loaded = __import__(data_loader)
            first_run = False
        else:
            importlib.reload(data_loaded)
        print("Pandas Data Imported, now Running Queries")
        
        # Setup SDQLpy environment
        if ("SDQLpy Setup" not in manifest_json) or (manifest_json["SDQLpy Setup"] == "False"):
            print("Skipping SDQLpy Setup")
        else:
            print("Setting up SDQLpy Environment")
            setup_sdqlpy(manifest_json["SDQLpy Setup"])
            print("SDQLpy Environment Imported")
        
        # We will run the converter to create our pandas files from our sql files
        # We want this to output the converted queries to a given directory
        # This directory is specified as: manifest_json["Temporary Directory"]
        
        def init_writer():
            temp_path = Path(manifest_json["Temporary Directory"])
            
            # Write an __init__.py into the file
            open(f"{temp_path}"+"/"+"__init__.py", 'a').close()
            
        # Delete if already exists
        def make_temp_folder():
            temp_path = Path(manifest_json["Temporary Directory"])
            if temp_path.exists() and temp_path.is_dir():
                try:
                    shutil.rmtree(temp_path)
                except:
                    # Sometimes due to AFS permissions we're not able to delete the directory
                    pass
            
            # Make a folder
            Path(temp_path).mkdir(parents=True, exist_ok=True)
            
            # Write an __init__.py into the file
            init_writer()
            
            return temp_path
        
        def delete_temp_folder():
            # Tear Down
            # Delete temporary folder
            if temp_path.exists() and temp_path.is_dir():
                try:
                    shutil.rmtree(temp_path)
                except:
                    # Sometimes due to AFS permission we're not able to delete the directory
                    pass
                
        def make_sql_file_path(manifest, query):
            file_path = None
            if manifest[-1] != "/" and query[0] != "/":
                file_path = manifest + "/" + query
            elif manifest[-1] == "/" and query[0] != "/":
                file_path = manifest + query
            elif manifest[-1] != "/" and query[0] == "/":
                file_path = manifest + query
            elif manifest[-1] == "/" and query[0] == "/":
                file_path = manifest + query[1:]
            else:
                raise ValueError("Manifest and Query JSON not recognised with paths: " + str(manifest) + " and " + str(query))
            return file_path
            
        temp_path = make_temp_folder()
    
        # Iterate through all the SQL Queries
        for query in manifest_json["Queries"]:
            temp_path = make_temp_folder()
            
            print("Doing Query: " + str(query["Query Name"]))
            
            # Store SQL and Pandas results
            sql_results_list = []
            execution_results_list = []            
            
            default_sql_file_path = make_sql_file_path(manifest_json["SQL Queries Location"], query["SQL Name"])
            
            # Create an array to store results
            results_array = []
            
            # Iterate through "Options"
            for query_option in query["Options"]:
                if "SQL Name" in query_option:
                    sql_file_path = make_sql_file_path(manifest_json["SQL Queries Location"], query_option["SQL Name"])
                else:
                    sql_file_path = default_sql_file_path
                
                if query_option["Type"] == "SQL":
                    sql_run_times = []
                    
                    sql_runs = manifest_json["Number of Query Runs"]
                    for i in range(sql_runs):
                        if args.verbose:
                            print("Doing " + str(query_option["DBMS"]) + " SQL Run: " + str(i+1))
                        
                        if query_option["DBMS"] == "Postgres":
                            sql_result, run_time = run_pg_query(pg_db_details, sql_file_path, args.verbose)
                        elif query_option["DBMS"] == "Duck DB":
                            sql_result, run_time = run_duck_query(duck_db_details, sql_file_path, args.verbose, number_of_threads)
                        elif query_option["DBMS"] == "Hyper DB":
                            sql_result, run_time = run_hyper_query(hyper_db_details, sql_file_path, args.verbose)
                        else:
                            raise Exception("Unrecognised Query runner option")
                        
                        sql_run_times.append(run_time)
                        sql_results_list.append((query_option["Results Name"], sql_result))
                        
                    if args.verbose:   
                        print(sql_run_times)
                    avg_3sf = round_sig(sum(sql_run_times)/len(sql_run_times), 3)
                    print(str(query_option["Results Name"]) + ": " + str(avg_3sf))
                    results_array.append(["SQL", str(scaling_factor), str(query_option["Results Name"]), str(query["Query Name"]), avg_3sf, str("Not added yet"), str("Yes"), sql_run_times])
                   
                
                elif query_option["Type"] == "Pandas":
                    if query_option["Converter"] == "True":
                        # We first convert the SQL to a Pandas Query, and then run it!
                        # Run converter
                        if query_option["Query Plan"] == "Postgres":
                            cmd = ["python3.10", manifest_json["SQL Converter Location"], '--file', sql_file_path, '--benchmarking', "True", "--output_location", manifest_json["Temporary Directory"], "--name", query_option["Converted Name"], "--query_planner", "Postgres", "--planner_file", manifest_json["Postgres Connection Details"]]
                        elif query_option["Query Plan"] == "Duck DB":
                            cmd = ["python3.10", manifest_json["SQL Converter Location"], '--file', sql_file_path, '--benchmarking', "True", "--output_location", manifest_json["Temporary Directory"], "--name", query_option["Converted Name"], "--query_planner", "Duck_DB", "--planner_file", duck_db_details]
                        elif query_option["Query Plan"] == "Hyper DB":
                            cmd = ["python3.10", manifest_json["SQL Converter Location"], '--file', sql_file_path, '--benchmarking', "True", "--output_location", manifest_json["Temporary Directory"], "--name", query_option["Converted Name"], "--query_planner", "Hyper_DB", "--planner_file", hyper_db_details]
                        else:
                            raise Exception("Unrecognised option")
                        
                        if "Conversion Options" in query_option:
                            if args.verbose:
                                print("Adding conversion options: " + str(query_option["Conversion Options"]))
                            cmd += query_option["Conversion Options"]
                        
                        # Use Numpy setting
                        set_numpy = False
                        if "Conversion Options" in query_option:
                            if "--use_numpy" in query_option["Conversion Options"]:
                                set_numpy = True
                        elif "Use Numpy" in manifest_json:
                            if manifest_json["Use Numpy"] == "True":
                                set_numpy = True
                            
                        if set_numpy == True:
                            # Only add numpy if not in cmd
                            if "--use_numpy" not in cmd:
                                cmd += ["--use_numpy", "True"]
                        
                        try:
                            if args.verbose:
                                print("We are running the converter, with the following command: " + str(cmd))
                            result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, timeout=600)
                            bad_query = False
                        except Exception as ex:
                            bad_query = True
                            # We print the error
                            # But still output to CSV and to the query file
                            print(color.RED + str(query["Query Name"]) + ": Pandas conversion error!" + "\n" + color.END)
                            print(ex)
                        
                        # Print returncode
                        if args.verbose:
                            print(f'The returncode for running the query was: {result.returncode}')
                        
                        if bad_query == False & result.returncode != 0:
                            # When we are unable to convert, handle the exception
                            bad_query = True
                            # We print the error
                            # But still output to CSV and to the query file
                            print(color.RED + str(query["Query Name"]) + ": Pandas conversion error!" + "\n" + color.END)
                            error_locations = [result.stdout, result.stderr]
                            for error_msg in error_locations:
                                if str(error_msg) != '':
                                    print(error_msg)
                            #raise Exception( f'Invalid result: { result.returncode }' )      
                        
                        query_name = str(sql_file_path.split("/")[-1]).split(".")[0]
                        function_default = "q" + query_name
                        
                        package_name = str(query_option["Converted Name"]).split(".")[0]
                        
                        if "Store Queries" in manifest_json:
                            if bad_query == True:
                                # Add the bad query content to the function
                                bad_content = "\n" + "def " + function_default + "():" + "\n" + "\t# Exception: Unable to convert query" + "\n" + "\tpass"
                                store_queries_function_body += bad_content
                            
                            else:
                                # Add to store_queries_function_body
                                f = open(path.join(manifest_json["Temporary Directory"], query_option["Converted Name"]), "r")
                                query_pandas_content = str(f.read())
                                
                                # Remove any content on first line between "(" and ")"
                                query_pandas_content = re.sub("\(.+?\)", "()", query_pandas_content, 1)
                                store_queries_function_body += query_pandas_content
                        
                        # Put an __init__.py file into the folder so it can be imported as a module
                        init_writer()
                        
                        # Set located directory, the directory storing the query
                        located_directory = manifest_json["Temporary Directory"]
                    elif query_option["Converter"] == "False":
                        # We don't convert, just use the file path specified to run it!    
                                        
                        # Set located directory, the directory storing the query
                        located_directory = manifest_json["Stored Queries Location"]
                        
                        # Set Package name
                        package_name = str(query_option["File Name"]).split(".")[0]
                        
                        # Set default function name
                        query_name = str(query["SQL Name"].split("/")[-1]).split(".")[0]
                        function_default = "q" + query_name

                        bad_query = False
                    else:
                        raise Exception("Unknown Converter option for a Pandas Type query: " + str(query_option["Converter"]))    
                    
                    # Change into the directory with the query, so as to import it
                    # Set changed Dirs to false
                    # If we don't have an incorrectly converted query
                    if bad_query == False:
                        changed_dirs = False
                        if "/" in str(located_directory):
                            # Split this, cd to the first part
                            split_dir = str(located_directory).split("/")
                            os.chdir(split_dir[0])
                            package_location = ".".join(split_dir[1:]) + ".%s" % package_name
                            
                            # We have changed directories
                            changed_dirs = True
                        else:
                            package_location = str(located_directory) + ".%s" % package_name
                        
                        bad_exec = False
                        
                        # if args.verbose and str(query["Query Name"]) in ["Query 8", "Query 12", "Query 14"]:
                        #     # Print out function content
                        #     with open(package_location.replace(".", "/") + ".py", 'r') as f:
                        #         print(f.read())                        
                        
                        try:
                            query_function = getattr(__import__(package_location, fromlist=[function_default]), function_default)
                        except Exception as ex:
                            print(color.RED + str(query["Query Name"]) + ": Pandas execution error!" + "\n" + color.END)
                            print(str(type(ex)) + " : " + str(ex))
                            bad_exec = True
                        
                        if bad_exec == False:
                            # Order of data imports
                            data_order = list(query_function.__code__.co_varnames[:query_function.__code__.co_argcount])
                            
                            # Get Query Data
                            query_data = [0] * len(data_order)
                            table_names = [0] * len(data_order)
                            for relation in query["Required Data"]:
                                # Get position of relation in data_order, use as position in query data
                                insert_pos = data_order.index(relation)
                                query_data[insert_pos] = getattr(data_loaded, relation)
                                table_names[insert_pos] = relation
                            
                            # Store Queries
                            if "Store Queries" in manifest_json:
                                store_queries_function_call += str(query_function.__name__) + "(" + ")"
                                store_queries_function_call += "\n"
                            
                            pandas_run_times = []
                            # Run the query and get an execution time for it
                            for i in range(manifest_json["Number of Query Runs"]):
                                if args.verbose:
                                    print("Doing Pandas Run: " + str(i+1))
                                
                                start_time = time.time()
                                try:
                                    query_function(*query_data)
                                except Exception as ex:
                                    print(color.RED + str(query["Query Name"]) + ": Pandas execution error!" + "\n" + color.END)
                                    print(str(type(ex)) + " : " + str(ex))
                                    bad_exec = True

                                end_time = time.time()
                                if args.verbose:
                                    print("\tRun time was: " + str(end_time - start_time))
                                pandas_run_times.append(end_time - start_time)
                                
                            # Run it once to get results
                            pandas_result = query_function(*query_data)
                            
                        else:
                            # Bad Exec == True
                            # Create a dummy pandas_run_times list
                            pandas_run_times = []
                            
                        # Change back if we've moved
                        if changed_dirs == True:
                            os.chdir("..")
                            changed_dirs = False
                        
                        if args.verbose:   
                            print(pandas_run_times)
                        avg_3sf = round_sig(sum(pandas_run_times)/len(pandas_run_times), 3)
                        print(str(query_option["Results Name"]) + ": " + str(avg_3sf))
                        if bad_exec == True:
                            results_array.append(["Pandas", str(scaling_factor), str(query_option["Results Name"]), str(query["Query Name"]), avg_3sf, str("Not added yet"), str("No"), pandas_run_times])
                        else:
                            results_array.append(["Pandas", str(scaling_factor), str(query_option["Results Name"]), str(query["Query Name"]), avg_3sf, str("Not added yet"), str("Yes"), pandas_run_times])
                        
                        # Append to execution_results_list, in a tuple
                        # If we were able to convert and run the query
                        if (bad_query == False) and (bad_exec == False):
                            execution_results_list.append((query_option["Results Name"], pandas_result))
                    else:
                        # It's a bad query, so do the bare minimum
                        
                        # Store Queries - Add to the function call
                        if "Store Queries" in manifest_json:
                            store_queries_function_call += str(function_default) + "(" + ")"
                            store_queries_function_call += "\n"
                            
                        # Add to results_array
                        results_array.append(["Pandas", str(scaling_factor), str(query_option["Results Name"]), str(query["Query Name"]), 0, str("Not added yet"), str("No"), [0]])
                
                elif query_option["Type"] == "SDQLpy":
                    # We first convert the SQL to a SDQLpy Query, and then run it!
                    # Run converter
                    
                    query_path = None
                    if ("Converter" in query_option and query_option["Converter"] == "True") or ("Converter" not in query_option):
                        if query_option["Query Plan"] == "Postgres":
                            cmd = ["python3.10", manifest_json["SQL Converter Location"], '--file', sql_file_path, '--benchmarking', "True", "--output_location", manifest_json["Temporary Directory"], "--name", query_option["Converted Name"], "--query_planner", "Postgres", "--planner_file", manifest_json["Postgres Connection Details"], "--output_fmt", "sdqlpy"]
                        elif query_option["Query Plan"] == "Duck DB":
                            cmd = ["python3.10", manifest_json["SQL Converter Location"], '--file', sql_file_path, '--benchmarking', "True", "--output_location", manifest_json["Temporary Directory"], "--name", query_option["Converted Name"], "--query_planner", "Duck_DB", "--planner_file", duck_db_details, "--output_fmt", "sdqlpy"]
                        elif query_option["Query Plan"] == "Hyper DB":
                            cmd = ["python3.10", manifest_json["SQL Converter Location"], '--file', sql_file_path, '--benchmarking', "True", "--output_location", manifest_json["Temporary Directory"], "--name", query_option["Converted Name"], "--query_planner", "Hyper_DB", "--planner_file", hyper_db_details, "--output_fmt", "sdqlpy"]
                        else:
                            raise Exception("Unrecognised option")
                        
                        if "Conversion Options" in query_option:
                            if args.verbose:
                                print("Adding conversion options: " + str(query_option["Conversion Options"]))
                            cmd += query_option["Conversion Options"]
                        
                        try:
                            if args.verbose:
                                print("We are running the converter, with the following command: " + str(cmd))
                            result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, timeout=600)
                            bad_query = False
                        except Exception as ex:
                            bad_query = True
                            # We print the error
                            # But still output to CSV and to the query file
                            print(color.RED + str(query["Query Name"]) + ": SDQLpy conversion error!" + "\n" + color.END)
                            print(ex)
                        
                        # Print returncode
                        if args.verbose:
                            print(f'The returncode for generating the query was: {result.returncode}')
                            
                        query_path = f"{manifest_json['Temporary Directory']}/{query_option['Converted Name']}"
                    elif ("Converter" in query_option and query_option["Converter"] == "False"):
                        # Use Stored Queries
                        assert "Stored Queries Location" in manifest_json
                        
                        print(f"Using a Stored Query, by the name of: {query_option['Converted Name']}")
                        
                        # Copy the file somewhere and have the path to the temporary one
                        storedPath = f"{manifest_json['Stored Queries Location']}/{query_option['Converted Name']}"
                        tempPath = f"{manifest_json['Temporary Directory']}/{query_option['Converted Name']}"
                        shutil.copy(storedPath, tempPath)
                        
                        query_path = tempPath
                    else:
                        raise Exception(f"Unrecognised Query Option Spec: {query_option}")    

                    try:
                        assert query_path != None
                        # Assert there is a file at query_path
                        assert os.path.isfile(query_path)
                        
                        sdqlpy_results = run_sdqlpy(query_path,
                                                    manifest_json["Number of Query Runs"],
                                                    manifest_json["SDQLpy Setup"],
                                                    manifest_json["Data Storage"],
                                                    number_of_threads)
                        bad_exec = False
                    except:
                        sdqlpy_results = dict()
                        sdqlpy_results["Times"] = []
                        sdqlpy_results["Result"] = []
                        bad_exec = True
                        print(color.RED + str(query["Query Name"]) + ": SDQLpy Execution error!" + "\n" + color.END)
                                            
                    sdqlpy_run_times = sdqlpy_results["Times"]
                               
                    if args.verbose:   
                        print(f"SDQLpy Run Times: {sdqlpy_run_times}")
                    if bad_exec == True:
                        results_array.append(["SDQLpy", str(scaling_factor), str(query_option["Results Name"]), str(query["Query Name"]), 0.0, str("Not added yet"), str("No"), sdqlpy_run_times])
                    else:
                        avg_3sf = round_sig(sum(sdqlpy_run_times)/len(sdqlpy_run_times), 3)
                        print(str(query_option["Results Name"]) + ": " + str(avg_3sf))
                        results_array.append(["SDQLpy", str(scaling_factor), str(query_option["Results Name"]), str(query["Query Name"]), avg_3sf, str("Not added yet"), str("Yes"), sdqlpy_run_times])
                    
                    # Append to execution_results_list, in a tuple
                    # If we were able to convert and run the query
                    if (bad_query == False) and (bad_exec == False):
                        execution_results_list.append((query_option["Results Name"], sdqlpy_results["Result"]))                     
                else:
                    raise Exception("Unable to Benchmark a query type: " + str(query_option["Type"]) + " that we are unfamiliar with.")
                
            # Checking correctness
            compare_decisions_list = []
            compare_decision = False
            for sql_name, sql_result in sql_results_list:
                assert sql_result != None
                for exec_name, exec_result in execution_results_list:
                    assert isinstance(exec_result, (pd.DataFrame, dict))
                    # We should check if exec_result is the same as sql_result
                    
                    compare_order_checking = None
                    if "Order Checking" in manifest_json:
                        compare_order_checking = manifest_json["Order Checking"]
                    else:
                        # Default is we check the order
                        compare_order_checking = True
                        
                    compare_decision, columns = compare(sql_file_path, exec_result, sql_result, manifest_json["Results Precision"], compare_order_checking)
                    compare_decisions_list.append(compare_decision)
                    
                    if not compare_decision:
                        print(color.RED + str(query["Query Name"]) + ": The returned data was not equivalent!" + "\n" + "Between " + str(exec_name) + " and " + str(sql_name) + "." + color.END)
                        print("Execution Data:")
                        print(exec_result)
                        print("SQL Data:")
                        print(columns)
                        for row in sql_result:
                            print(row)
                    else:
                        print(color.GREEN + str(query["Query Name"]) + ": The returned data was correct and the same!" + "\n" + "\tBetween " + str(exec_name) + " and " + str(sql_name) + "." + color.END)
                        # if args.verbose:
                        #     print("Pandas Data:")
                        #     print(exec_result)
                        #     print("SQL Data:")
                        #     print(columns)
                        #     for row in sql_result:
                        #         print(row)
                        
            # Write results, use from results_array
            for result_set in results_array:
                # Edit the result_set, 5th element, with the correctness decision (compare_decision)
                local_result_set = result_set
                local_result_set[5] = compare_decision
            
                results_writer(manifest_json, local_result_set)
                
            # If all compare decisions were true, then print out equivalent
            if (all(compare_decisions_list) == True) and (compare_decisions_list != []):
                print(color.BOLD + str(query["Query Name"]) + ": The returned data was equivalent for both SQL and Pandas" + color.END)
                
            delete_temp_folder()

    print(color.GREEN + "Testing is complete and results have been written to: " + str(manifest_json["Results Location"]) + color.END)    
    
    # Writing out store queries
    if "Store Queries" in manifest_json:
        # store_queries_file_content
        
        # Do replacements with "Rename Tables"
        if "Rename Tables" in manifest_json["Store Queries"]:
            # Iterate through tables
            for table in manifest_json["Store Queries"]["Rename Tables"]:
                # Do replace
                # Replace for a relation, relation+".", is the more likely form
                store_queries_function_body = store_queries_function_body.replace(table+".", manifest_json["Store Queries"]["Rename Tables"][table]+".")
                # Also replace the form: relation+"["
                store_queries_function_body = store_queries_function_body.replace(table+"[", manifest_json["Store Queries"]["Rename Tables"][table]+"[")
        
        store_queries_file_content += store_queries_function_body
        store_queries_file_content += "\n\n"
        store_queries_file_content += "# ====================="
        store_queries_file_content += "\n\n"
        
        store_queries_file_content += store_queries_function_call
        
        # Add imports 
        store_queries_file_content = store_queries_file_content.replace("import pandas as pd", "").replace("import numpy as np", "")
        start_store_queries_file_content = "import pandas as pd\n"
        
        # Use Numpy
        if "Use Numpy" in manifest_json:
            if manifest_json["Use Numpy"] == "True":
                start_store_queries_file_content += "import numpy as np\n"
        
        start_store_queries_file_content += store_queries_file_content
        start_store_queries_file_content += "\n"
        
        # Write out to file, named "Name"
        with open(manifest_json["Store Queries"]["Name"], "w+") as f:
            f.write(start_store_queries_file_content)
        
    delete_temp_folder()
    # TODO: Add back in once solve bugs
    # teardown_sdqlpy(manifest_json["SDQLpy Setup"]["Location"])
    
    # Restore HyperThreading
    os.system('echo on | tee /sys/devices/system/cpu/smt/control >/dev/null 2>&1')
    
if __name__ == "__main__":
    main()
