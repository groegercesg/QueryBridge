import sys
# Add the parent directory of package to sys.path before attempting to import anything from package using absolute imports:
from pathlib import Path # if you haven't already done so
current_file = Path(__file__).resolve()
parent, root = current_file.parent, current_file.parents[1]
sys.path.append(str(root))

from os import listdir
from os.path import isfile, join
import json

from prepare_databases.prepare_hyperdb import PrepareHyperDB
from hyper_parsing import generate_unparse_content_from_explain_and_query
from tpch_helpers import *
from sdqlpy_optimisations import sdqlpy_apply_optimisations

query_directory = 'sql_to_pandas/tpch_no_limit_order_with_aggrs'
explain_directory = 'sql_to_pandas/hyperdb_tpch_explain_no_limit_order_with_aggrs'

def generate_hyperdb_explains():
    db = PrepareHyperDB('hyperdb_tpch.hyper')
    db.prepare_database('data_storage')
    
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
    onlyfiles = [f for f in listdir(explain_directory) if isfile(join(explain_directory, f))]
    
    operators = set()
    
    for explain_file in onlyfiles:
        with open(f'{explain_directory}/{explain_file}') as r:
            explain_content = json.loads(r.read())

        operators.update(gather_operators(explain_content))
    
    print("Below are the operators that Hyper DB Uses:")
    print(operators)

def convert_explain_plan_to_x(desired_format):
    # Ignore Query 15 for now
    query_files = [f for f in listdir(query_directory) if isfile(join(query_directory, f)) and str(f).split(".")[0] != "15"]
    explain_files = [f for f in listdir(explain_directory) if isfile(join(explain_directory, f))]
    
    combined_sql_content = list(zip(query_files, explain_files))
    
    
    supported_queries = ["1", "2", "3" ,"4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15_cte", "16", "17", "18", "19", "20", "21", "22"]
    
    print(f"We currently support {len(supported_queries)} out of a total of 22")
    
    for sql_file, explain_file in combined_sql_content:
        if sql_file.split(".")[0] not in supported_queries:
            continue
        
        with open(f'{explain_directory}/{explain_file}') as r:
            explain_content = json.loads(r.read())
            
            table_schema = configure_table_schema({})
            
            uplan_opts = "" #uplan_opts = ["ColumnElimination"]
            
            unparse_content = generate_unparse_content_from_explain_and_query(
                explain_content,
                f'{query_directory}/{sql_file}',
                desired_format,
                table_schema,
                uplan_opts
            )
            
            print(unparse_content)
            content_size = 0
            if desired_format == "pandas":
                content_size = len(unparse_content.getPandasContent())
            elif desired_format == "sdqlpy":
                # Do Optimisations
                # TODO: We need to make these optimisations interact correctly with the removeColumnIDs
                unparse_content.sdqlpy_tree = sdqlpy_apply_optimisations(unparse_content.sdqlpy_tree, ["VerticalFolding", "PipelineBreaker"]) #
                
                content_size = len(unparse_content.getSDQLpyContent())
            else:
                raise Exception("Unrecognised desired format")
            
            assert content_size > 0
            print(f"Generated {content_size} lines of SDQLpy code")

# generate_hyperdb_explains()
# # inspect_explain_plans()
# # parse_explain_plans()
convert_explain_plan_to_x("sdqlpy") # sdqlpy || pandas
