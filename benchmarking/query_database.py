import psycopg2
import json
import time
import psycopg2.extensions
from psycopg2.extras import LoggingConnection, LoggingCursor
import logging

logging.basicConfig(level=logging.DEBUG)
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

manifest_json = json.load(open("basic_test.json"))


db_details = manifest_json["Database Connection Details"]

# Read SQL file
with open(manifest_json["SQL Queries"][0]["Query Location"], 'r') as file:
    sql_query = file.read()

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

    print("Executing SQL Query")
    cursor.execute(sql_query)
    retrieved_records = cursor.fetchall()

    print("Result of Query")
    for row in retrieved_records:
        print(row)
        
    print("Execution time was: " + str(connection.exec_time))
except (Exception, psycopg2.Error) as error:
    print("Error while fetching data from PostgreSQL", error)
finally:
    # closing database connection.
    if connection:
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")