import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
 
import pandas_tree
import pandas_tree_to_pandas  
import sql_to_pandas.prepare_postgres as prepare_postgres