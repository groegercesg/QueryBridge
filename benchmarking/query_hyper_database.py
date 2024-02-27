import time
import subprocess
from prepare_databases.prepare_hyperdb import PrepareHyperDB
import os

def run_hyper_query(db_details, query_file, verbose):
    # Read SQL file
    with open(query_file, 'r') as file:
        sql_query = file.read()
        
    exec_time = 0.0
    results = []
    queries = None
    
    # Process our SQL queries into an array of queries
    if sql_query.count(";") > 1:
        # Many queries in one file
        queries = sql_query.split(";")
        for i in range(len(queries)):
            queries[i] = queries[i].strip()
            queries[i] = queries[i] + ";"
            if queries[i] == ";":
                del queries[i]
    else:
        queries = [sql_query]

    # Try query, catch error
    try:
        db = PrepareHyperDB(db_details)
        
        # HyperThreading
        if str(os.getenv("NO_HYPER_THREADING")) != "1":
            os.system('echo on | tee /sys/devices/system/cpu/smt/control >/dev/null 2>&1')
        else:
            os.system('echo off | tee /sys/devices/system/cpu/smt/control >/dev/null 2>&1')
        
        for i, single_query in enumerate(queries):
            if verbose:
                print("Executing SQL Query, part", i+1, "of", len(queries), ".")
            
            exec_time += db.execute_timed_query(single_query)
        
        # Run at end to get results
        retrieved_records = db.execute_query(single_query)
        results.append(retrieved_records)
            
        os.system('echo on | tee /sys/devices/system/cpu/smt/control >/dev/null 2>&1')
    except Exception as error:
        print("Error while fetching data from Hyper DB: ", error)
    
    # Choose results
    if len(results) == 1:
        results = results[0]
        assert (results != [[None]]) or (results != [None]) or (results != None)
    else:
        for individual_res in results:
            if individual_res != []:
                results = individual_res
                break
        #raise ValueError("We have multiple statements that return values, we haven't coded how to handle this.")
            
    return results, exec_time
