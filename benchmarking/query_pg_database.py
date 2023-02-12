import psycopg2
import time
import psycopg2.extensions
from psycopg2.extras import LoggingConnection, LoggingCursor
import logging

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
        connection = psycopg2.connect(user=db_details["User"],
                                    password=db_details["Password"],
                                    host=db_details["Host"],
                                    port=db_details["Port"],
                                    database=db_details["Database"],
                                    connection_factory=MyLoggingConnection)
        connection.initialize(logger)
        cursor = connection.cursor()
        
        for i, single_query in enumerate(queries):
            if verbose:
                print("Executing SQL Query, part", i+1, "of", len(queries), ".")
            
            cursor.execute(single_query)
            
            if single_query[:6].lower() == "select":
                retrieved_records = cursor.fetchall()
                results.append(retrieved_records)
            
            exec_time += connection.exec_time
    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgreSQL", error)
    finally:
        # closing database connection.
        if connection:
            cursor.close()
            connection.close()
            if verbose:
                print("PostgreSQL connection is closed")
    
    # Choose results
    if len(results) == 1:
        results = results[0]
    else:
        raise ValueError("We have multiple statements that return values, we haven't coded how to handle this.")
            
    return results, exec_time

# For testing
if __name__ == "__main__":
    pass
    import json
    manifest_json = json.load(open('query_15_test.json'))

    run_query(manifest_json["Database Connection Details"], "queries/15.sql", True)
