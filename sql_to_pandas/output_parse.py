# Use the lark package to parse strings from the Output into a tree-based class representation
from os import listdir
from os.path import isfile, join
import json
import re

from prepare_duckdb import prep_duck
from lark_parsing import parse_larks
from plan_to_explain_tree import *
from lark import Transformer

def generate_duckdb_explains():
    db = prep_duck('duckdb_tpch.duckdb')
    explain_opts = "EXPLAIN (COSTS FALSE, VERBOSE TRUE, FORMAT JSON) "
    query_directory = 'sql_to_pandas/tpch_no_limit_order_with_aggrs'
    explain_directory = 'sql_to_pandas/duck_tpch_explain_no_limit_order_with_aggrs'
    
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

def make_tree_from_duck(explain_path):
    with open(explain_path, "r") as f:
        explain_content = json.loads(f.read())["children"][0]
       
    # Fix JSON, it should be split by newline 
    duck_fix_extra_info(explain_content)

    explain_tree = make_class_tree_from_duck(explain_content)
    
    if isinstance(explain_tree, list):
        print(explain_tree[0])
    else:
        print(explain_tree)
    
    # TODO: Fix Tree, it has '#0' references in it
    # Visit the explain_tree in bottom_up order
    postorder_traversal(explain_tree, ReplaceColumnReferences)
    
    # TODO: After doing this (^), remove Projection Nodes
    
    #print(explain_tree.output)

def postorder_traversal(root: base_node, transformer: Transformer):
    #if root is None return
    if root==None:
        return
    
    # Run on children
    for child in root.plans:
        postorder_traversal(child, transformer)
        
    # Work on Root
    # Skip the leaves - these shouldn't have column replacements 
    if len(root.plans) > 0:
        #print(root.node_type)
        for index, output_tree in enumerate(root.output):
            root.output[index] = transformer(root.plans).transform(output_tree)

class ReplaceColumnReferences(Transformer):
    column_reference_regex = r"^#[0-9]+$"
    
    def __init__(self, child_trees: list[base_node]):
        self.child_trees = child_trees
    
    def string(self, token):
        assert len(token) == 1
        regex_search = re.search(self.column_reference_regex,token[0])
        if regex_search != None:
            column_target = int(token[0].strip().replace('#', ''))
            assert len(self.child_trees[0].output) - 1 >= column_target, f"Child does not have the required length ({column_target})"
            #print(f'We need to do col_ref replacing on {token[0]}')
            
            # Return the new element
            # TODO: This is hardcoded logic, we can determine the appropriate paradigm in the future
            return self.child_trees[0].output[column_target]
        else:
            # Return the existing element
            return token

#def duck_resolve_column_references(tree):

class ReferenceTracker():
    changes = False
    def changed(self):
        self.changes = True
    def reset(self):
        self.changes = False

INFO_SEPARATOR = "[INFOSEPARATOR]"
AGG_FUNCTIONS = {"sum", "avg", "count", "max", "min", "distinct", "mean", "count_star"}
 
def duck_fix_extra_info(json):
    json["extra_info"] = list(filter(None, json["extra_info"].split('\n')))
    
    # Check if this node has a child
    if "children" in json:
        if json["children"] != []:
            for individual_plan in json['children']:
                duck_fix_extra_info(individual_plan)

def duck_fix_explain_references_controller(json):
    # We should run this function repeatedly until it doesn't change anything
    ref_tracker = ReferenceTracker()
    duck_fix_explain_references(json, ref_tracker)
    while ref_tracker.changes == True:
        ref_tracker.reset()
        duck_fix_explain_references(json, ref_tracker)

def duck_fix_explain_references(json, ref_tracker):
    # Column reference regex
    col_ref_regex = r"\#[0-9]*(?!.*')"
    
    for index, col in enumerate(json['extra_info']):
        regex_search = re.search(col_ref_regex, col)
        if regex_search != None:
            col_replace = regex_search.group(0)
            col_index = int(col_replace.replace("#", ""))
            
            if len(json['children']) != 1:
                raise Exception("Have to implement a determine_local_child style method")
            
            # Get child_value
            child = json['children'][0]
            while len(child['extra_info']) - 1 < col_index:
                child = child['children'][0]
            child_value = child['extra_info'][col_index]
            
            # Do replacement 
            json['extra_info'][index] = json['extra_info'][index].replace(col_replace, child_value)
            
            # Set replaced variable to true
            ref_tracker.changed()
                                        
    # Check if this node has a child
    if "children" in json:
        if json["children"] != []:
            for individual_plan in json['children']:
                duck_fix_explain_references(individual_plan, ref_tracker)

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
    extra_info = json["extra_info"]
        
    if node_type == "simple_aggregate":
        node_class = aggregate_node("Aggregate", parse_larks(extra_info))
    elif node_type == "projection":
        node_class = projection_node("Projection", parse_larks(extra_info))
    elif node_type == "seq_scan":
        node_class = process_seq_scan(extra_info)
    elif node_type == "order_by":
        node_class = sort_node("Sort", [], parse_larks(extra_info))
    elif node_type in ["hash_group_by", "perfect_hash_group_by"]:
        group_keys = []
        for col in extra_info:
            if not any([True for agg in AGG_FUNCTIONS if agg+"(" in col]):
                group_keys.append(col)
        node_class = group_aggregate_node("Group Aggregate", parse_larks(extra_info), parse_larks(group_keys))
    elif node_type == "top_n":
        # We start with a limit node
        node_class = limit_node("Limit", [])
        # Below this we have a sort_node
        start_keys = 0
        for idx, elem in enumerate(extra_info):
            if elem == INFO_SEPARATOR:
                start_keys = idx
                break
        
        sort_node_object = sort_node("Sort", [], parse_larks(extra_info[start_keys+1:]))
        node_class.set_plans([sort_node_object])
    elif node_type in ['hash_join', 'piecewise_merge_join', 'delim_join']:
        hash_join_output = []
        join_type = extra_info[0]
        inner_unique = False
        node_class = hash_join_node("Hash Join", hash_join_output, inner_unique, join_type, parse_larks(extra_info[1:]))
    elif node_type == "limit":
        node_class = limit_node("Limit", parse_larks(extra_info))
    elif node_type == "filter":
        node_class = filter_node("Filter", parse_larks(extra_info))
    elif node_type == "chunk_scan":
        node_class = chunk_scan_node("Chunk Scan", parse_larks(extra_info))
    elif node_type == "delim_scan":
        node_class = delim_scan_node('Delim Scan', parse_larks(extra_info))
    else:
        raise Exception(f"Node Type: '{node_type}' is not recognised, many Node Types have not been implemented.")
    
    # Check if this node has a parent
    if parent != None and node_class != None:
        # We have a parent
        node_class.set_parent(parent)
        
    # Check if this node has a child
    node_class_plans = []
    if "children" in json:
        if json["children"] != []:
            for individual_plan in json['children']:
                sub_plans = make_class_tree_from_duck(individual_plan, node_class)
                if isinstance(sub_plans, list):
                    node_class_plans.extend(sub_plans)
                else:
                    node_class_plans.append(sub_plans)
            
            if node_class != None:
                lowest_node = node_class
                while lowest_node.plans != []:
                    if len(lowest_node.plans) != 1:
                        raise Exception(f'Adding to node with children, but multiple. See here: {lowest_node}')
                    lowest_node = lowest_node.plans[0]
                lowest_node.set_plans(node_class_plans)
    
    if node_class != None:
        return node_class
    else:
        return node_class_plans
    
    
def set_node_names(file):
    with open(file, "r") as f:
        explain_content = json.loads(f.read())["children"][0]
        
    local_set = set()
    local_jts = set()
    
    nodes_to_review = []
    nodes_to_review.append(explain_content)
    while len(nodes_to_review) > 0:
        current_node = nodes_to_review.pop()
        local_set.add(current_node["name"])
        if current_node["name"] in ["HASH_JOIN", "DELIM_JOIN", "PIECEWISE_MERGE_JOIN"]:
            local_jts.add(current_node["extra_info"].split("\n")[0])
        
        if "children" in current_node:
            for child_node in current_node["children"]:
                nodes_to_review.append(child_node)
        
    return local_set, local_jts


def gather_all_nodes():
    explain_directory = 'sql_to_pandas/duck_tpch_explain_no_limit_order_with_aggrs'
    onlyfiles = [f for f in listdir(explain_directory) if isfile(join(explain_directory, f))]
    
    targeted_nodes = {'DELIM_JOIN', 'DELIM_SCAN', 'PROJECTION', 'HASH_JOIN', 'CHUNK_SCAN', 'HASH_GROUP_BY', 'FILTER', 'SEQ_SCAN'}
    supportable_queries = []
    
    overall_set = set()
    join_types = set()
    for explain_file in onlyfiles:
        query_nodes, query_jts = set_node_names(f'{explain_directory}/{explain_file}')
        overall_set.update(query_nodes)
        join_types.update(query_jts)
        
        if query_nodes.issubset(targeted_nodes):
            supportable_queries.append(explain_file)
        
    print('-'*15)
    print(overall_set)
    print('-'*15)
    print(join_types)
    print('-'*15)
    print(f"{len(supportable_queries)} Can be supported with this combination of nodes")
    print(f"They are:")
    for q in supportable_queries:
        print(f"\t{q}")

def run_tree_generation():
    explain_directory = 'sql_to_pandas/duck_tpch_explain'
    onlyfiles = [f for f in listdir(explain_directory) if isfile(join(explain_directory, f))]
    
    # Track failures
    failed = []
    for explain_file in onlyfiles:
        print(f'Doing: {explain_file}')
        try:
            make_tree_from_duck(f'{explain_directory}/{explain_file}')
        except:
            print('\tFAILED')
            failed.append(explain_file)
    
    print('-'*15)
    print(f'We have been able to parse {len(onlyfiles) - len(failed)}/{len(onlyfiles)} explain JSONs.')
    if len(failed) > 0:
        print('We failed:')
        for failed_file in failed:
            print(f'\t{failed_file}')

#generate_duckdb_explains()
#run_tree_generation()

#make_tree_from_duck(f'sql_to_pandas/duck_tpch_explain/{1}_duck.json')
#set_node_names(f'sql_to_pandas/duck_tpch_explain/{1}_duck.json')

gather_all_nodes()

# print(set_node_names(f'sql_to_pandas/duck_tpch_explain_no_limit_order_with_aggrs/{21}_duck.json'))


