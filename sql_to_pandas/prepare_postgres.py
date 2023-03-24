import json
import psycopg2
import os
import sys

class prep_pg():
    def __init__(self, connection_details):
        # Read connection details from file to json
        with open(connection_details) as f:
            self.connection_details = json.load(f)
        # Initialise a connection object
        self.connection = self.open_connection()
        
        # Store TPC-H tables
        self.tables = ['LINEITEM', 'PARTSUPP', 'ORDERS', 'CUSTOMER', 'SUPPLIER', 'NATION', 'REGION', 'PART']
        
    def open_connection(self):
        return psycopg2.connect(user=self.connection_details["User"],
                                    password=self.connection_details["Password"],
                                    host=self.connection_details["Host"],
                                    port=self.connection_details["Port"],
                                    database=self.connection_details["Database"])
        
    def clean_database(self):
        """
        Using the initialise connection object, clean all tables at this database
        Drops the tables if they exist
        Return:
            0 if successful
            non zero otherwise
        """
        cursor = self.connection.cursor()
        try:
            for table in self.tables:
                cursor.execute("DROP TABLE IF EXISTS %s " % table)
            self.connection.commit()
        except Exception as e:
            print("Unable to remove existing tables. %s" % e)
            return 1
        cursor.close()
        return 0
    
    def is_database_empty(self):
        # Return TRUE if database has no tables at connection obj, FALSE if it has tables
        cursor = self.connection.cursor()
        
        cursor.execute("""SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public'""")

        cursor_fetch = cursor.fetchall()
        cursor.close()
        
        if len(cursor_fetch) > 0:
            return False
        else:
            print(cursor_fetch)
            return True
        
    def execute_query(self, query):
        # Execute a query
        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
        except psycopg2.errors.UndefinedTable:
            # THis has happened, but we need to close and re-open the connection
            self.connection.close()
            self.connection = self.open_connection()
        except Exception as ex:
            raise Exception(ex)
        finally:
            cursor.close()
             
    def get_explain(self, query, query_name=None):
        # Return explain data as json

        cursor = self.connection.cursor()
        cursor.execute(query)
        
        cursor_fetch = cursor.fetchall()[0][0][0]
        
        cursor.close()
        return cursor_fetch
    
    def prepare_test_database(self, data_dir, constants_dir):
        self.data_dir = data_dir
        # Load Data
            # Clean Database
        if self.clean_database():
            print("Could not clean the database.")
            exit(1)
        print("Cleaned database %s" % self.connection_details["Database"])
            # Create Schema
        if self.create_schema(constants_dir):
            print("Could not create schema.")
            exit(1)
        print("Done creating schemas")
            # Load Tables
        if self.load_tables():
            print("Could not load data to tables")
            exit(1)
        print("Done loading data to tables")    
        
        # Don't index tables, we have cut our data up so indexes and foreign keys won't work
        
        print("-"*15)
        print("Complete: Database is prepared and loaded")
        
    def prepare_database(self, data_dir, constants_dir):
        # Set the data dir
        self.data_dir = data_dir    
        
        # Load Data
            # Clean Database
        if self.clean_database():
            print("Could not clean the database.")
            exit(1)
        print("Cleaned database %s" % self.connection_details["Database"])
            # Create Schema
        if self.create_schema(constants_dir):
            print("Could not create schema.")
            exit(1)
        print("Done creating schemas")
            # Load Tables
        if self.load_tables():
            print("Could not load data to tables")
            exit(1)
        print("Done loading data to tables")    
            # Index Tables
        if self.index_tables(constants_dir):
            print("Could not create indexes for tables")
            exit(1)
        print("Done creating indexes and foreign keys")
        
        print("Complete: Database is prepared and loaded")
        
    def index_tables(self, constants_dir):
        """
        Creates indexes and foreign keys for loaded tables.
        Return:
            0 if successful
            non zero otherwise
        """
        cursor = self.connection.cursor()
        try:
            with open(os.path.join(constants_dir, "create_idx.sql")) as query_file:
                query = query_file.read()
                cursor.execute(query)
            self.connection.commit()
        except Exception as e:
            print("Unable to run index tables. %s" % e)
            return 1
        cursor.close()
        return 0
                 
    def load_tables(self):
        """
        Loads data into tables. Expects that tables are already empty and initialised.
        Return:
            0 if successful
            non zero otherwise
        """
        cursor = self.connection.cursor()
        try:
            for table in self.tables:
                filepath = os.path.join(self.data_dir, table.lower() + ".tbl.csv")
                with open(filepath, 'r') as in_file:
                    cursor.copy_from(in_file, table=table.lower(), sep="|")
            self.connection.commit()
        except Exception as e:
            print("Unable to run load tables. %s" %e)
            return 1
        cursor.close()
        return 0
            
    def create_schema(self, constants_dir):
        """
        Creates the schema for the tables.
        Return:
            0 if successful
            non zero otherwise
        """
        cursor = self.connection.cursor()
        try:
            with open(os.path.join(constants_dir, "create_tbl.sql")) as query_file:
                query = query_file.read()
                cursor.execute(query)
            self.connection.commit()
        except Exception as e:
            print("Unable to run create tables. %s" % e)
            return 1
        cursor.close()
        return 0

def actually_setup_database(db_conn_file, scaling_factor, db_gen, data_storage, constants_loc):
    # Prepare the database
    
    print("Preparing Database")
    
    db = prep_pg(db_conn_file)

    db.prepare_database(data_storage, constants_loc)

    print("Database prepared")
