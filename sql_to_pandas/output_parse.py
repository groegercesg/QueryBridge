# Use the lark package to parse strings from the Output into a tree-based class representation
import lark
from lark import ParseTree
from prepare_duckdb import prep_duck
from os import listdir
from os.path import isfile, join
import json
import re

from plan_to_explain_tree import *

def generate_duckdb_explains():
    db = prep_duck('duckdb_tpch.duckdb')
    explain_opts = "EXPLAIN (COSTS FALSE, VERBOSE TRUE, FORMAT JSON) "
    query_directory = 'sql_to_pandas/tpch_queries'
    explain_directory = 'sql_to_pandas/tpch_explain'
    
    onlyfiles = [f for f in listdir(query_directory) if isfile(join(query_directory, f))]
    
    for query_file in onlyfiles:
        with open(f'{query_directory}/{query_file}') as r:
            explain_content = explain_opts.rstrip('\r\n') + '\n' + r.read()
            
        query_name = query_file.split('.')[0]
    
        # Else, we can request the explain data from the database
        explain_json = db.get_explain(explain_content, query_name)

        # Write out explain_content to explain_file_path
        with open(f'{explain_directory}/{query_name}_duck.json', "w") as outfile:
            # returns JSON object as a dictionary, write that to outfile
            outfile.write(json.dumps(explain_json, indent=4))
            
        print(f'Generated explain output for: {query_file}')
        
    print(f'Generated Explain output for all {len(onlyfiles)} queries.')

def parse_larks(texts: list[str]) -> list[ParseTree]:
    lark_parse_trees = []
    for text in texts:
        lark_parse_trees.append(parse_lark(text))
    return lark_parse_trees

def parse_lark(text):
    parser = lark.Lark.open('grammars/duck.lark', rel_to=__file__, parser="earley", start="start")

    tree = parser.parse(text)

    return tree

def make_tree_from_duck():
    explain_path = 'sql_to_pandas/tpch_explain/6_duck.json'
    with open(explain_path, "r") as f:
        explain_content = json.loads(f.read())["children"][0]
    
    # Fix JSON, it has '#0' references, edit in-place
    duck_fix_explain_references(explain_content)

    explain_tree = make_class_tree_from_duck(explain_content)
    
    print(explain_tree.output[0])
    
def duck_fix_explain_references(json):
    # Column reference regex
    col_ref_regex = r"\#[0-9]*(?!.*')"
    
    regex_search = re.search(col_ref_regex, json['extra_info'])
    if regex_search != None:
        col_replace = regex_search.group(0)
        col_index = int(col_replace.replace("#", ""))
        
        if len(json['children']) != 1:
            raise Exception("Have to implement a determine_local_child style method")
        
        # Get child_value
        child_value = json['children'][0]['extra_info'].split('\n')[col_index]
        
        # Do replacement 
        json['extra_info'] = json['extra_info'].replace(col_replace, child_value)
                                    
    # Check if this node has a child
    if "children" in json:
        if json["children"] != []:
            for individual_plan in json['children']:
                duck_fix_explain_references(individual_plan)
    
INFO_SEPARATOR = "[INFOSEPARATOR]"

# Specialist process methods
def process_seq_scan(extra_info):
    # Set the relation and alias
    if extra_info[0] != INFO_SEPARATOR:
        relation_name = extra_info[0]
        alias_name = extra_info[0]
        
    start_projections = 0
    for idx, elem in enumerate(extra_info):
        if elem == INFO_SEPARATOR:
            start_projections = idx
            break
        
    end_projections = None
    # Check if there is a second info_separator, belies the presence of filters
    for idx, elem in enumerate(extra_info[start_projections+1:]):
        if elem == INFO_SEPARATOR:
            end_projections = idx + start_projections + 1
            break
        
    projections = []
    if end_projections == None:
        # No end, just use the rest
        projections = extra_info[start_projections+1:]
    else:
        projections = extra_info[start_projections+1:end_projections]
        
    node_class = seq_scan_node("Seq Scan", parse_larks(projections), relation_name, alias_name)
    
    # Were there filters?
    if end_projections != None:
        filter_part = extra_info[end_projections+1:]
        if filter_part[0][:9].lower() == "filters: ":
            filter_part[0] = filter_part[0][9:]
            
            # We can't just take to the end
            # There might be another infoseparator
            for idx, elem in enumerate(filter_part):
                if elem == INFO_SEPARATOR:
                    raise Exception(f"There was another info separator, why? See here: {filter_part}")
            
            larked_filters = parse_larks(filter_part)
            node_class.add_filters(larked_filters)
        else:
            raise Exception(f"We detected that there were filters, but they weren't in the standard form. See here: {filter_part}")
    return node_class

def make_class_tree_from_duck(json, parent=None):
    node_type = json["name"].lower()
    extra_info = json["extra_info"].split('\n')
        
    if node_type == "simple_aggregate":
        node_class = aggregate_node("Aggregate", parse_larks(extra_info))
    elif node_type == "projection":
        # Make a projection node an aggregate node
        node_class = aggregate_node("Aggregate", parse_larks(extra_info))
        node_class.add_remove_later(True)
    elif node_type == "seq_scan":
        node_class = process_seq_scan(extra_info)
    else:
        raise Exception(f"Node Type: '{node_type}' is not recognised, many Node Types have not been implemented.")
    
    # Check if this node has a parent
    if parent != None:
        # We have a parent
        node_class.set_parent(parent)
        
    # Check if this node has a child
    if "children" in json:
        if json["children"] != []:
            node_class_plans = []
            for individual_plan in json['children']:
                node_class_plans.append(make_class_tree_from_duck(individual_plan, node_class))
            
            node_class.set_plans(node_class_plans)
    
    return node_class
    

make_tree_from_duck()