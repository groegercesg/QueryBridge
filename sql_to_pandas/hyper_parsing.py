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
    
    if explain_node["operator"] == "select":
        print("a")
    
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

from hyper_classes import *

SUPPORTED_OPERATORS = {'leftsemijoin', 'map', 'sort', 'join', 'groupby', 'leftantijoin', 'groupjoin', 'rightsemijoin', 'executiontarget', 'select', 'rightantijoin', 'tablescan'}

def str_to_class(classname):
    return getattr(sys.modules[__name__], classname)

def create_operator_tree(explain_json):
    operator_name = explain_json["operator"].lower()
    if operator_name not in SUPPORTED_OPERATORS:
        raise Exception(f"Unknown operator in explain json: {operator_name}")
    
    operator_class = None
    if operator_name == "executiontarget":
        operator_class = executiontargetNode(explain_json["output"], explain_json["outputNames"])
    elif operator_name == "groupby":
        keyExpressions = []
        if "keyExpressions" in explain_json:
            keyExpressions = explain_json["keyExpressions"]
        aggExpressions = []
        if "aggExpressions" in explain_json:
            aggExpressions = explain_json["aggExpressions"]
        operator_class = groupbyNode(keyExpressions, aggExpressions, explain_json["aggregates"])
    elif operator_name == "tablescan":
        tableRestrictions = []
        if "restrictions" in explain_json:
            tableRestrictions = explain_json["restrictions"]
        operator_class = tablescanNode(explain_json["debugName"]["value"], explain_json["values"], tableRestrictions)
    elif operator_name == "sort":
        operator_class = sortNode(explain_json["criterion"])
    elif operator_name == "map":
        operator_class = mapNode(explain_json["values"])
    elif operator_name == "select":
        operator_class = selectNode(explain_json["condition"])
    elif operator_name == "join":
        operator_class = joinNode(explain_json["method"], explain_json["condition"])
    elif operator_name == "groupjoin":
        operator_class = groupjoinNode(explain_json["semantic"],
                                       explain_json["leftKey"],
                                       explain_json["rightKey"],
                                       explain_json["leftExpressions"],
                                       explain_json["leftAggregates"],
                                       explain_json["rightExpressions"],
                                       explain_json["rightAggregates"])
    elif operator_name == "leftsemijoin":
        operator_class = leftsemijoinNode(explain_json["method"], explain_json["condition"])
    elif operator_name == "leftantijoin":
        operator_class = leftantijoinNode(explain_json["method"], explain_json["condition"])
    elif operator_name == "rightsemijoin":
        operator_class = rightsemijoinNode(explain_json["method"], explain_json["condition"])
    elif operator_name == "rightantijoin":
        operator_class = rightantijoinNode(explain_json["method"], explain_json["condition"])
    else:
        raise Exception(f"No Operator Class Creation has been defined for the {operator_name} operator.")

    # Validate children operator options are as expected
    if ("input" in explain_json) and (any(x in explain_json for x in ["left", "right"])):
        raise Exception("There shouldn't be 'input' as well as either 'left'/'right' parameters for an operator")
    
    # Add children to the different operators
    if ("input" in explain_json):
        operator_class.addChild(create_operator_tree(explain_json["input"]))
    elif any(x in explain_json for x in ["left", "right"]):
        # It should be an operator type if we're here
        assert isinstance(operator_class, JoinNode) and hasattr(operator_class, "isJoinNode") and operator_class.isJoinNode == True
        operator_class.addLeft(create_operator_tree(explain_json["left"]))
        operator_class.addRight(create_operator_tree(explain_json["right"]))
    else:
        # Only table scan node should have no children
        assert isinstance(operator_class, tablescanNode)
    
    return operator_class
    
def parse_explain_plans():
    explain_directory = 'sql_to_pandas/hyperdb_tpch_explain'
    
    onlyfiles = [f for f in listdir(explain_directory) if isfile(join(explain_directory, f))]
    
    all_operator_trees = []
    for explain_file in onlyfiles:
        print(f"Transforming {explain_file} into a Hyper Tree")
        with open(f'{explain_directory}/{explain_file}') as r:
            explain_content = json.loads(r.read())
        
        op_tree = create_operator_tree(explain_content)
        all_operator_trees.append(op_tree)

    assert len(all_operator_trees) == 21
    print("All 21 HyperDB plans have been parsed into Hyper DB Class Trees")
    
    # Transform the operator_trees
    # Task 1: Solve the 'v1' references, in 'iu'
        # These propagate up, and are occasionally reset by things like "mapNode" or "groupbyNode"
        # Postorder traversal is required for this, make a dict of the pairs:
            # v1: l_suppkey
            # v3: p_partkey
            # ...
    # Task 2: ...

#generate_hyperdb_explains()
#inspect_explain_plans()
parse_explain_plans()
