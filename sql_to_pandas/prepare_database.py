import json
import psycopg2

class prep_db():
    def __init__(self, connection_details, dbgen_path = None):
        # Read connection details from file to json
        with open(connection_details) as f:
            self.connection_details = json.load(f)
        # Initialise a connection object
        self.connection = self.open_connection()
        
        # Set dbgen_path
        self.dbgen_path = dbgen_path
        
    def open_connection(self):
        return psycopg2.connect(user=self.connection_details["User"],
                                    password=self.connection_details["Password"],
                                    host=self.connection_details["Host"],
                                    port=self.connection_details["Port"],
                                    database=self.connection_details["Database"])
        
    def clean_database(self):
        # Using the initialise connection object, clean all tables at this database
        pass
    
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
    
    def prepare_database(self, scaling_factor = 1):
        # Prepare the database, at the connection object!
        
        if self.dbgen_path == None:
            raise Exception("Please specify a location of a dbgen generator.")
        pass
    