import psycopg2
import time
import psycopg2.extensions
from psycopg2.extras import LoggingConnection, LoggingCursor
import logging
from prepare_databases.prepare_postgres import PreparePostgres
import os

logging.basicConfig(level=logging.CRITICAL)
logger = logging.getLogger(__name__)

# MyLoggingCursor simply sets self.timestamp at start of each query                                                                 
class MyLoggingCursor(LoggingCursor):
    def execute(self, query, vars=None):
        self.timestamp = time.time()
        return super(MyLoggingCursor, self).execute(query, vars)

    def callproc(self, procname, vars=None):
        self.timestamp = time.time()
        return super(MyLoggingCursor, self).callproc(procname, vars)

# MyLogging Connection:                                                                                                             
#   a) calls MyLoggingCursor rather than the default                                                                                
#   b) adds resulting execution (+ transport) time via filter()                                                                     
class MyLoggingConnection(LoggingConnection):
    def filter(self, msg, curs):
        self.exec_time = time.time() - curs.timestamp
        
    def cursor(self, *args, **kwargs):
        kwargs.setdefault('cursor_factory', MyLoggingCursor)
        return LoggingConnection.cursor(self, *args, **kwargs)

def run_pg_query(db_details, query_file, verbose):
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

    # Try connection, catch error
    try:
        db = PreparePostgres(db_details, MyLoggingConnection)
        #db.connection.initialize(logger)
        
        # HyperThreading
        if str(os.getenv("NO_HYPER_THREADING")) != "1":
            os.system('echo on | tee /sys/devices/system/cpu/smt/control >/dev/null 2>&1')
        else:
            os.system('echo off | tee /sys/devices/system/cpu/smt/control >/dev/null 2>&1')
        
        for i, single_query in enumerate(queries):
            if verbose:
                print("Executing SQL Query, part", i+1, "of", len(queries), ".")
            
            # retrieved_records = db.execute_query(single_query)
            
            start = time.time()
            
            db.execute_query(single_query)
            
            end = time.time()
            
            # # If it's a select query, we store the results
            # if (single_query[:6].lower() == "select") or (single_query[:4].lower() == "with"):
            #     results.append(retrieved_records)
            
            # exec_time += db.connection.exec_time
            
            # Increment running counter
            exec_time += (end - start)
        
        # Run at end to get results
        retrieved_records = db.execute_query(single_query)
        results.append(retrieved_records)
            
        os.system('echo on | tee /sys/devices/system/cpu/smt/control >/dev/null 2>&1')
    except (Exception, psycopg2.Error) as error:
        print(f"Error while fetching data from PostgreSQL: {error}")
    finally:
        # closing database connection.
        db.connection.close()
        if verbose:
            print("PostgreSQL connection is closed")
    
    # Choose results
    if len(results) == 1:
        results = results[0]
    else:
        print(results)
        raise ValueError("We have multiple statements that return values, we haven't coded how to handle this.")
            
    return results, exec_time
