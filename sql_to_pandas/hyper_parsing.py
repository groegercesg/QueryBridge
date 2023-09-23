# Use the lark package to parse strings from the Output into a tree-based class representation
import sys
# Add the parent directory of package to sys.path before attempting to import anything from package using absolute imports:
from pathlib import Path # if you haven't already done so
current_file = Path(__file__).resolve()
parent, root = current_file.parent, current_file.parents[1]
sys.path.append(str(root))
from prepare_databases.prepare_hyperdb import PrepareHyperDB
from os import listdir
from os.path import isfile, join
import json

def generate_hyperdb_explains():
    db = PrepareHyperDB('hyperdb_tpch.hyper')
    db.prepare_database('data_storage')
    query_directory = 'sql_to_pandas/tpch_queries'
    explain_directory = 'sql_to_pandas/hyperdb_tpch_explain'
    
    onlyfiles = [f for f in listdir(query_directory) if isfile(join(query_directory, f))]
    
    for query_file in onlyfiles:
        with open(f'{query_directory}/{query_file}') as r:
            explain_content = r.read()
            
        query_name = query_file.split('.')[0]
        
        # TODO: Skip Query 15:
        # HyperException: Views are disabled.
        if str(query_name) == "15":
            continue
    
        # Else, we can request the explain data from the database
        explain_json, query_content = db.get_explain(explain_content, query_name)

        # Write out explain_content to explain_file_path
        with open(f'{explain_directory}/{query_name}_hyper.json', "w") as outfile:
            # returns JSON object as a dictionary, write that to outfile
            outfile.write(json.dumps(explain_json, indent=4))
            
        print(f'Generated explain output for: {query_file}')
        
    print(f'Generated Explain output for all {len(onlyfiles)} queries.')

def gather_operators(explain_node):
    operators = set()
    has_below = False
    for option in ["input", "left", "right"]: 
        if option in explain_node:
            has_below = True
            operators.update(gather_operators(explain_node[option]))
    
    if has_below == False and explain_node["operator"] != "tablescan":
        raise Exception(f"New leaf node discovered: { explain_node['operator'] }")
    
    operators.add(explain_node["operator"])
    return operators

def inspect_explain_plans():
    explain_directory = 'sql_to_pandas/hyperdb_tpch_explain'
    
    onlyfiles = [f for f in listdir(explain_directory) if isfile(join(explain_directory, f))]
    
    operators = set()
    
    for explain_file in onlyfiles:
        with open(f'{explain_directory}/{explain_file}') as r:
            explain_content = json.loads(r.read())

        operators.update(gather_operators(explain_content))
    
    print("Below are the operators that Hyper DB Uses:")
    print(operators)

generate_hyperdb_explains()
inspect_explain_plans()
