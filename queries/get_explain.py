import json
import shlex
import subprocess

what_query = str(3)
query_folder = "q"+what_query
file = query_folder + "/"+ what_query+".sql"
explain_file = query_folder + "/"+ what_query+"_explain.sql"
output_file = query_folder + "/"+ "q"+what_query+"_explain.json"
tree_output = query_folder + "/"+ "Q"+what_query+"_explain_tree"
tree_prune_output = query_folder + "/"+ "Q"+what_query+"_explain_post_prune_tree"
tree_pandas_output = query_folder + "/"+ "Q"+what_query+"_pandas_tree"
command = "psql -d tpchdb -U tpch -a -f " + explain_file

json_file = open(output_file, "w")
# Run command
cmd = subprocess.run(shlex.split(command), check=True, stdout=json_file)
json_file.close()

# Clean-up output_file, make into nice JSON and load
# Remove before "------"
with open(output_file, 'r+') as fp:
    # read an store all lines into list
    lines = fp.readlines()
    # move file pointer to the beginning of a file
    fp.seek(0)
    # truncate the file
    fp.truncate()
    
    start_line = 0
    for i, line in enumerate(lines):
        if "----------------------------------" in line:
            start_line = i
            break
    
    # after start line
    # E.g.: lines[1:] from line 2 to last line
    # And trim the final few lines
    lines = lines[start_line+1:len(lines)-2]
    
    # Remove "+" from lines
    for i in range(len(lines)):
        lines[i] = lines[i][:-2] + '\n'
        lines[i] = lines[i].strip()

    # Append bracket to last line
    lines[len(lines) - 1] = lines[len(lines) - 1] + "]"

    # start writing lines 
    fp.writelines(lines)
    
# Load json from written file
explain_json = ""
f = open(output_file)
  
# returns JSON object as a dictionary
explain_json = json.load(f)[0]
f.close()

from plan_to_explain_tree import * 
# print(json.dumps(explain_json, indent=4))

explain_tree = None


def json_to_class(json, tree):
    # First node check
    if tree == None:
        node = json["Plan"]
    
    else:
        node = json

    node_class = None    
    node_type = node["Node Type"]
    if node_type.lower() == "limit":
        node_class = limit_node(node_type, node["Parallel Aware"], node["Async Capable"], node["Output"])
    elif node_type.lower() == "aggregate":
        if "Group Key" in node:
            node_class = group_aggregate_node("Group " + node_type, node["Parallel Aware"], node["Async Capable"], node["Output"], node["Strategy"], node["Partial Mode"], node["Parent Relationship"], node["Group Key"])
        else:
            node_class = aggregate_node(node_type, node["Parallel Aware"], node["Async Capable"], node["Output"], node["Strategy"], node["Partial Mode"], node["Parent Relationship"])
    elif node_type.lower() == "gather":    
        node_class = gather_node(node_type, node["Parallel Aware"], node["Async Capable"], node["Output"], node["Workers Planned"], node["Single Copy"], node["Parent Relationship"])
    elif node_type.lower() == "seq scan":
        node_class = seq_scan_node(node_type, node["Parallel Aware"], node["Async Capable"], node["Output"], node["Relation Name"], node["Schema"], node["Alias"], node["Parent Relationship"], node["Filter"])
    elif node_type.lower() == "sort":
        node_class = sort_node(node_type, node["Parallel Aware"], node["Async Capable"], node["Output"], node["Sort Key"], node["Parent Relationship"])
    elif node_type.lower() == "nested loop":
        node_class = nested_loop_node(node_type, node['Parallel Aware'], node['Async Capable'], node['Output'], node['Inner Unique'], node['Join Type'], node['Parent Relationship']) 
    elif node_type.lower() == "hash join":
        node_class = hash_join_node(node_type, node['Parallel Aware'], node['Async Capable'], node['Output'], node['Inner Unique'], node['Join Type'], node['Hash Cond'], node['Parent Relationship'])
    elif node_type.lower() == "hash":
        node_class = hash_node(node_type, node['Parallel Aware'], node['Async Capable'], node['Output'], node['Parent Relationship'])
    elif node_type.lower() == "index scan":
        node_class = index_scan_node(node_type, node['Parallel Aware'], node['Async Capable'], node['Scan Direction'], node['Index Name'], node['Relation Name'], node['Schema'], node['Alias'], node['Index Cond'], node['Filter'], node['Output'], node['Parent Relationship'])
    else:
        raise Exception("Node Type", node_type, "is not recognised, many Node Types have not been implemented.")
        
    # Check if this node has a child
    if "Plans" in node:
        node_class_plans = []
        for individual_plan in node['Plans']:
            node_class_plans.append(json_to_class(individual_plan, ""))
        node_class.set_plans(node_class_plans)
    
    return node_class
   
# Build a class structure that is nested within each other
explain_tree = json_to_class(explain_json, explain_tree) 

# Let's try and visualise the explain tree now
from visualising_tree import plot_tree, plot_pandas_tree
plot_tree(explain_tree, tree_output)

from explain_tree import solve_nested_loop_node, solve_hash_node
solve_nested_loop_node(explain_tree)
solve_hash_node(explain_tree)

# Plot tree after pruning/altering, show changes in tree
plot_tree(explain_tree, tree_prune_output)

# Let's try create a pandas list
from pandas_tree import make_pandas_tree
pandas_tree = make_pandas_tree(explain_tree, file)

# Make tree of pandas!
plot_pandas_tree(pandas_tree, tree_pandas_output)

# Let's try and write some pandas code from this
from pandas_tree_to_pandas import make_pandas
pandas = make_pandas(pandas_tree, file)

for statement in pandas:
    print(statement)