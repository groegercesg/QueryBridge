import json
import psycopg2
import os
import subprocess
from subprocess import DEVNULL, STDOUT, PIPE
import glob
import re

class prep_db():
    def __init__(self, connection_details, dbgen_path = None):
        # Read connection details from file to json
        with open(connection_details) as f:
            self.connection_details = json.load(f)
        # Initialise a connection object
        self.connection = self.open_connection()
        
        # Set dbgen_path
        self.dbgen_path = dbgen_path
        
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
             
    def get_explain(self, query):
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
        
    
    def prepare_database(self, data_dir, constants_dir, scaling_factor = 1):
        # Prepare the database, at the connection object!
        self.scaling_factor = scaling_factor
        self.data_dir = data_dir
        
        if self.dbgen_path == None:
            raise Exception("Please specify a location of a dbgen generator.")
    
        # Prepare Data
            # Build DBGEN
        if self.build_dbgen():
            print("Could not build the dbgen.")
            exit(1)
        print("Built dbgen from source.")
            # Generate Data
        # try to generate data files
        if self.generate_data():
            print("Could not generate data files.")
            exit(1)
        print("Created data files in %s" % data_dir)
        
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
    
            
    def generate_data(self):
        """
        Generates the data from dbgen, and moves it into the data_dir directory.
        Return:
            0 if successful
            non zero otherwise
        """
        cur_dir = os.getcwd()
        
        # Change into the dbgen dir
        os.chdir(self.dbgen_path)
        
        # Delete all tbl files in folder
        for in_fname in glob.glob("*.tbl"):
            os.remove(in_fname)
        
        p = subprocess.Popen([os.path.join(".", "dbgen"), "-vf", "-s", str(self.scaling_factor)], stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
        
        if not p.returncode:
            # We have generated the data files successfully, but they're all in the dbgen_path
            # We should move them to self.data_dir
                try:
                    # TODO: This is hardcoded and sloppy, but it works for now
                    os.makedirs("../" + self.data_dir, exist_ok=True)
                    # Match anything in self.dbgen_path with an extension of .tbl
                    for in_fname in glob.glob("*.tbl"):
                        # Change the file permissions
                        os.chmod(in_fname, 0o777)
                        fname = os.path.basename(in_fname)
                        out_fname = os.path.join("../" + self.data_dir, fname + ".csv")
                        try:
                            with open(in_fname) as in_file, open(out_fname, "w") as out_file:
                                for inline in in_file:
                                    outline = re.sub("\|$", "", inline)
                                    out_file.write(outline)
                            os.remove(in_fname)
                        except IOError as e:
                            print("Something bad happened while transforming data files. (%s)" % e)
                            # Change back out of it
                            os.chdir(cur_dir)
                            return 1
                except IOError as e:
                    print("Unable to create data directory %s. (%s)" % (self.data_dir, e))
                    # Change back out of it
                    os.chdir(cur_dir)
                    return 1
                
                # All files written successfully. Return success code.
                # Change back out of it
                os.chdir(cur_dir)
                return 0
        else:
            
            # Change back out of it
            os.chdir(cur_dir)
            print(out)
            print(err)
            
            # TODO:
            # Sometimes we get here, because a file is locked, permission issues
            # We can resolve this by deleting all the "*.tbl" files in self.dbgen_path
            # And rerunning this current function
            return p.returncode
    
    def build_dbgen(self):
        """
        Compiles the dbgen from source.
        The Makefile must be present in the same directory as this script.
        Return:
            0 if successful
            non zero otherwise
        """
        cur_dir = os.getcwd()
        
        # Change into the dbgen dir
        os.chdir(self.dbgen_path)
        
        p = subprocess.Popen(["make", "-f", os.path.join(cur_dir, "makefile")], stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
        
        # Change back out of it
        os.chdir(cur_dir)
        
        if not p.returncode:
            return p.returncode
        else:
            print(out)
            print(err)
            return p.returncode
