import json
import shlex
import subprocess

file = "1.sql"
explain_file = "1_explain.sql"
output_file = "q1_explain.json"
tree_output = "Q1_explain_tree"
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
    else:
        raise Exception("Node Type", node_type, "is not recognised, many Node Types have not been implemented.")
        
    # Check if this node has a child
    if "Plans" in node:
        node_class.set_plans(json_to_class(node["Plans"][0], ""))
    
    return node_class
   
# Build a class structure that is nested within each other
explain_tree = json_to_class(explain_json, explain_tree) 

# print(explain_tree)

# Let's try and visualise the explain tree now
from visualising_tree import plot_tree
plot_tree(explain_tree, tree_output)

# Let's try create a pandas list
from pandas_list import make_pandas_list
pandas_list = make_pandas_list(explain_tree, file)

# Let's try and write some pandas code from this
from pandas_list_to_pandas import make_pandas
pandas = make_pandas(pandas_list)


# Plans to get around limitations
    # Doesn't give us names of columns
        # Top class has output, this lines up with the .sql file output, so we can scan and get it from that
    # Doesn't have the number to limit by
        # Get this from the sql as well

for statement in pandas:
    print(statement)