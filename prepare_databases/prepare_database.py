from abc import ABC, abstractmethod

EXPLAIN_LOCATION = 1

class PrepareDatabase(ABC):
    
    """
    Required methods:
        is_database_empty
        get_explain
        execute_query
        prepare_database
    """
    def __init__(self, connection_details, database_name):
        self.connection_details = connection_details
        self.database_name = database_name
        # Store TPC-H tables
        self.tables = ['LINEITEM', 'PARTSUPP', 'ORDERS', 'CUSTOMER', 'SUPPLIER', 'NATION', 'REGION', 'PART']        
    
    @abstractmethod
    def is_database_empty(self):
        raise Exception("Abstract method")
    
    @abstractmethod
    def get_explain(self, query_text, query_name=None):
        raise Exception("Abstract method")
    
    """
    Execute query will by default return the 'fetchall' version of the
    executed query
    """
    @abstractmethod
    def execute_query(self, query_text):
        raise Exception("Abstract method")
    
    @abstractmethod
    def prepare_database(self, data_dir, constants_dir=None):
        raise Exception("Abstract method")
    
    def create_explain(self, explain_list: list) -> str:
        # We have a list, where one of the elements
        if isinstance(explain_list, list):
            for idx, content in enumerate(explain_list):
                if content == EXPLAIN_LOCATION:
                    explain_list[idx] = self.explain_options
            return ' '.join(explain_list)
        else:
            raise Exception(f"Unknown format for the explain_list variable, it was of type: { type(explain_list) }")