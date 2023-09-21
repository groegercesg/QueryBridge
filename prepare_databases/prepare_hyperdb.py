from prepare_databases.prepare_database import PrepareDatabase
from tableauhyperapi import HyperProcess, Telemetry, CreateMode, Connection, TableDefinition, SqlType

class PrepareHyperDB(PrepareDatabase):
    def __init__(self, connection_details):
        super().__init__(connection_details, "Hyper DB")
        self.explain_options = "EXPLAIN (VERBOSE)"
        self.hyper_parameters = {
            "log_config": "",
            "max_query_size": "10000000000",
            "hard_concurrent_query_thread_limit": "1",
            "inital_compilation_mode": "o"
        }
        
    def __open_connection(self):
        with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU, parameters=self.hyper_parameters) as hyper:
            return Connection(hyper.endpoint, self.connection_details, CreateMode.NONE)
    
    def is_database_empty(self):
        tables = []
        with self.__open_connection() as connection:
            catalog = connection.catalog
            schemas = catalog.get_schema_names()
            for schema_name in schemas:
                tables.extend(catalog.get_table_names(schema_name))
        if len(tables) > 0:
            return False
        else:
            print(tables)
            return True
    
    def execute_query(self, query_text):
        with self.__open_connection() as connection:
            return connection.execute_list_query(query_text)
    
    def get_explain(self, query_text, query_name=None):
        query_text = self.create_explain(query_text)
        return self.execute_query(query_text), query_text
        
    def prepare_database(self, data_dir, constants_dir=None):
        # Create the database, and replace if exists
        assert self.__create_database == True
        print("Created new Database file, at: " + str(self.connection_details))
        
        # Create TableDefinitions
        tables = [
            TableDefinition('region', [
                TableDefinition.Column('r_regionkey', SqlType.int),
                # MORE
            ])
        ]
        
        
        with self.__open_connection() as connection:
            # Now populate it with tables and data
            for table in tables:
                connection.catalog.create_table(table_definition=table)
            print("Created tables")
            
            # Create 'assumed' primary key
            # Create 'assumed' foreign key
            # See Link: https://github.com/tableau/hyper-api-samples/blob/7ff43088c5db678dec9b9e1cb2a8be1ad952100d/Community-Supported/publish-multi-table-hyper/publish-multi-table-hyper.py#L63
            
            # Load data
            # Use an Inserter
            
        assert self.is_database_empty() == False
        
        print("HyperDB Database creation completed")
            
    def __create_database(self):
        with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU, parameters=self.hyper_parameters) as hyper:
            with Connection(hyper.endpoint, self.connection_details, CreateMode.CREATE_AND_REPLACE) as connection:
                return connection.is_open and connection.is_ready
    