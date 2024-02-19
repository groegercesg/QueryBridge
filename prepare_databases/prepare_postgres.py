from prepare_databases.prepare_database import PrepareDatabase
import json
import psycopg2
import os

class PreparePostgres(PrepareDatabase):
    def __init__(self, connection_details, number_of_threads = 1, connection_factory=None):
        # connection details might be a dict
        if isinstance(connection_details, dict):
            pass
        # Or if it's just a str, we try to open a file from it
        elif isinstance(connection_details, str):
            with open(connection_details) as f:
                connection_details = json.load(f)
        else:
            raise Exception(f"Unknown format for connection_details: {type(connection_details)}")
            
        super().__init__(connection_details, "Postgres")
        self.connection = self.__open_connection(connection_factory)
        self.explain_options = "EXPLAIN (COSTS FALSE, VERBOSE TRUE, FORMAT JSON)"
    
    def __open_connection(self, connection_factory=None):
        try:
            connection = psycopg2.connect(user=self.connection_details["User"],
                                    password=self.connection_details["Password"],
                                    host=self.connection_details["Host"],
                                    port=self.connection_details["Port"],
                                    database=self.connection_details["Database"],
                                    connection_factory=connection_factory)
        except Exception as ex:
            raise Exception(ex)
        return connection
    
    def is_database_empty(self):
        # Return TRUE if database has no tables at connection obj, FALSE if it has tables
        query_fetch = self.execute_query("""SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public'""")
        
        if len(query_fetch) > 0:
            return False
        else:
            print(query_fetch)
            return True
        
    def execute_query(self, query_text):
        query_fetch = None
        with self.connection.cursor() as cursor:
            try:
                cursor.execute(query_text)
                if cursor.description != None:
                    query_fetch = cursor.fetchall()
                else:
                    query_fetch = []
            except psycopg2.errors.UndefinedTable:
                self.connection.close()
                self.connection = self.__open_connection()
            except Exception as ex:
                raise Exception(ex)
        return query_fetch
        
    def get_explain(self, query_text, query_name=None):
        query_text = self.create_explain(query_text)
        # Return explain data as json
        desired_json = self.execute_query(query_text)[0][0][0]
        
        return desired_json, query_text
    
    def prepare_database(self, data_dir, constants_dir=None):# Set the data dir
        self.data_dir = data_dir    
        
        # Load Data
            # Clean Database
        if self.__clean_database():
            print("Could not clean the database.")
            exit(1)
        print("Cleaned database %s" % self.connection_details["Database"])
            # Create Schema
        if self.__create_schema(constants_dir):
            print("Could not create schema.")
            exit(1)
        print("Done creating schemas")
            # Load Tables
        if self.__load_tables():
            print("Could not load data to tables")
            exit(1)
        print("Done loading data to tables")    
            # Index Tables
        if self.__index_tables(constants_dir):
            print("Could not create indexes for tables")
            exit(1)
        print("Done creating indexes and foreign keys")
        
        print("Complete: Database is prepared and loaded")
    
    def __clean_database(self):
        """
        Using the initialise connection object, clean all tables at this database
        Drops the tables if they exist
        Return:
            0 if successful
            non zero otherwise
        """
        with self.connection.cursor() as cursor:
            try:
                for table in self.tables:
                    cursor.execute("DROP TABLE IF EXISTS %s " % table)
                self.connection.commit()
            except Exception as e:
                print("Unable to remove existing tables. %s" % e)
                return 1
        return 0
    
    def __create_schema(self, constants_dir):
        """
        Creates the schema for the tables.
        Return:
            0 if successful
            non zero otherwise
        """
        with self.connection.cursor() as cursor:
            try:
                with open(os.path.join(constants_dir, "create_tbl.sql")) as query_file:
                    query = query_file.read()
                    cursor.execute(query)
                self.connection.commit()
            except Exception as e:
                print("Unable to run create tables. %s" % e)
                return 1
        return 0
    
    def __load_tables(self):
        """
        Loads data into tables. Expects that tables are already empty and initialised.
        Return:
            0 if successful
            non zero otherwise
        """
        with self.connection.cursor() as cursor:
            try:
                for table in self.tables:
                    filepath = os.path.join(self.data_dir, table.lower() + ".tbl.csv")
                    with open(filepath, 'r') as in_file:
                        cursor.copy_from(in_file, table=table.lower(), sep="|")
                self.connection.commit()
            except Exception as e:
                print("Unable to run load tables. %s" %e)
                return 1
        return 0
    
    def __index_tables(self, constants_dir):
        """
        Creates indexes and foreign keys for loaded tables.
        Return:
            0 if successful
            non zero otherwise
        """
        with self.connection.cursor() as cursor:
            try:
                with open(os.path.join(constants_dir, "create_idx.sql")) as query_file:
                    query = query_file.read()
                    cursor.execute(query)
                self.connection.commit()
            except Exception as e:
                print("Unable to run index tables. %s" % e)
                return 1
        return 0
