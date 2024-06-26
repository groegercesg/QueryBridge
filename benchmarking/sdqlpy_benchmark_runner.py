import time
from sdqlpy.fastd import *
from sdqlpy.sdql_lib import *

import json
import os
import io
import sys
import subprocess

def bench_runner(iterations, func, args, query_columns, data_write_path):
    times_list = []
    
    # HyperThreading
    if str(os.getenv("NO_HYPER_THREADING")) != "1":
        os.system('echo on | tee /sys/devices/system/cpu/smt/control >/dev/null 2>&1')
    else:
        os.system('echo off | tee /sys/devices/system/cpu/smt/control >/dev/null 2>&1')
    
    # Run 'iteration' number of times
    for i in range(0, iterations):
        start=(time.time())
        func(*args)
        end=(time.time())
        times_list.append(end-start)
    
    # Run at end to gather results
    results = func(*args)

    print("Executed the query")
    
    os.system('echo on | tee /sys/devices/system/cpu/smt/control >/dev/null 2>&1')
    
    # Create JSON dict
    result_dict = dict()
    
    if isinstance(results, float):
        assert len(query_columns) == 1
        result_dict[query_columns[0]] = [str(results)]
    elif isinstance(results, fastd):
        temp_location = 'temporary_fastd_data.txt'
        
        try:
            os.remove(temp_location)
        except OSError:
            pass

        f = open(temp_location,'w')
        stdout = sys.stdout.fileno()
        saved = os.dup(stdout)

        redirect_stdout( f.fileno(), stdout )
        not_important = str(results)
        redirect_stdout( saved, stdout )

        with open(temp_location, "r") as f:
            results_str = f.read()
            
        try:
            os.remove(temp_location)
        except OSError:
            pass
        
        results_split = results_str.split(" -> true, ")
        # Strip start and end
        results_split[0] = results_split[0][2:]
        results_split[-1] = results_split[-1].split(" -> true")[0]
        
        # Initialise the columns to an empty array
        for y in query_columns:
            result_dict[y] = []
        
        for x in results_split:
            # And remove ope/close, Split on comma
            assert x[0] == "<" and x[-1] == ">"
            inner_result = x[1:-1]
            inner_result = inner_result.replace('\x00','')
            inner_split = inner_result.split("||")
            
            assert len(inner_split) == len(query_columns), f"inner_split: {inner_split} ({len(inner_split)}) and query_columns: {query_columns} ({len(query_columns)})"
            # Stuff into results_dict
            for idx, y in enumerate(inner_split):
                result_dict[query_columns[idx]].append(y)
        
    elif isinstance(results, sr_dict):
        results_str = str(results)
        print(results_str)
        raise Exception("Not implemented yet, sr_dict")
    else:
        raise Exception(f"Results is of unknown type: {type(results)}")
    
    # Create JSON data
    json_data = dict()
    json_data['Times'] = times_list
    json_data['Result'] = result_dict
        
    # Write results to location
    with open(data_write_path, 'w') as f:
        json.dump(json_data, f)

def redirect_stdout(to_fd, stdout):
    """Redirect stdout to the given file descriptor."""
    sys.stdout.close()
    os.dup2(to_fd, stdout)
    sys.stdout = io.TextIOWrapper(os.fdopen(stdout, 'wb'))