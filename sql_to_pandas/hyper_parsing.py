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
    
        # Else, we can request the explain data from the database
        explain_json = db.get_explain(explain_content, query_name)

        # Write out explain_content to explain_file_path
        with open(f'{explain_directory}/{query_name}_hyper.json', "w") as outfile:
            # returns JSON object as a dictionary, write that to outfile
            outfile.write(json.dumps(explain_json, indent=4))
            
        print(f'Generated explain output for: {query_file}')
        
    print(f'Generated Explain output for all {len(onlyfiles)} queries.')

generate_hyperdb_explains()