import json

# Flag for timing
timing=True
what_query = str(10)
query_folder = "q"+what_query
file = query_folder + "/"+ what_query+".sql"
explain_file = query_folder + "/"+ what_query+"_explain.sql"
output_file = query_folder + "/"+ "q"+what_query+"_explain.json"
tree_output = query_folder + "/"+ "Q"+what_query+"_explain_tree"
tree_prune_output = query_folder + "/"+ "Q"+what_query+"_explain_post_prune_tree"
tree_pandas_output = query_folder + "/"+ "Q"+what_query+"_pandas_tree"
command = "psql -d tpchdb -U tpch -a -f " + explain_file

from clean_up_json import run
# Execute the command, get the json and clean it
run(command, output_file)

# Load json from written file
explain_json = ""
f = open(output_file)

# returns JSON object as a dictionary
explain_json = json.load(f)[0]
f.close()

# Print out the json
# print(json.dumps(explain_json, indent=4))

from explain_tree import make_tree
# Build a class structure that is nested within each other
explain_tree = None
explain_tree = make_tree(explain_json, explain_tree)

# Let's try and visualise the explain tree now
from visualising_tree import plot_tree, plot_pandas_tree
plot_tree(explain_tree, tree_output)

# Prune and alter the tree, for later use
from explain_tree import solve_nested_loop_node, solve_hash_node
solve_nested_loop_node(explain_tree)
solve_hash_node(explain_tree)

# Plot tree after pruning/altering, show changes in tree
plot_tree(explain_tree, tree_prune_output)

# Let's try create a pandas tree
from pandas_tree import make_pandas_tree
pandas_tree = make_pandas_tree(explain_tree, file)

# Make tree of pandas!
plot_pandas_tree(pandas_tree, tree_pandas_output)

# Let's try and write some pandas code from this
from pandas_tree_to_pandas import make_pandas
pandas = make_pandas(pandas_tree, file, timing)

# Print out the pandas code, line by line
for statement in pandas:
    print(statement)
