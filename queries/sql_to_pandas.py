import json
import shutil
import sys
from pathlib import Path
import argparse
import shlex
import subprocess

def str2bool(v):
    if isinstance(v, bool):
        return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')

def init_argparse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        usage="%(prog)s [OPTION] [FILE]...",
        description="Convert SQL Queries to Pandas DataFrame Statements."
    )
    parser.add_argument(
        "-v", "--version", action="version",
        version = f"{parser.prog} version 0.2.0"
    )
    parser.add_argument('--file',
                       metavar='file',
                       type=str,
                       help='The file we would like to convert')
    
    parser.add_argument('--benchmarking',
                       metavar='benchmarking',
                       type=str2bool,
                       nargs='?',
                       const=True, 
                       default=False,
                       help='Whether we are benchmarking or not, controls the output of various additional information')
    
    parser.add_argument('--output_location',
                       metavar='output_location',
                       type=str,
                       help='The location that we should output the file to')
    
    parser.add_argument('--name',
                       metavar='output_name',
                       type=str,
                       help='The name of the file that we should output')
    
    parser.add_argument('--column_ordering',
                    metavar='column_ordering',
                    type=str2bool,
                    nargs='?',
                    const=True, 
                    default=False,
                    help='Whether we would like our column ordering to be perfectly accurate or not. Has significant impact on run-time.')

    parser.add_argument('--column_limiting',
                    metavar='column_limiting',
                    type=str2bool,
                    nargs='?',
                    const=True, 
                    default=True,
                    help='Whether we would like our columns between nodes to be limited, can help with ongoing memory usage.')

    return parser


def line_prepender(filename, line):
    with open(filename, 'r+') as f:
        content = f.read()
        f.seek(0, 0)
        f.write(line.rstrip('\r\n') + '\n' + content)
        
def do_main_pandas_compilation(python_output_name, tree_pandas_output, query_file, args, tree, relations_subqueries, treeHelp, set_value=None, output_index=None):
    from pandas_tree import make_pandas_tree
    from visualising_tree import plot_pandas_tree
    from pandas_tree_to_pandas import make_pandas
    
    # Let's try create a pandas tree
    pandas_tree = make_pandas_tree(tree, query_file)
    
    # Make visual tree of pandas!
    if not args.benchmarking:
        if output_index != None:
            plot_pandas_tree(pandas_tree, tree_pandas_output+"_"+str(output_index))
        else:
            plot_pandas_tree(pandas_tree, tree_pandas_output)
    
    
    if set_value != None:
        # We have a sub_query on our hands, the final node needs to set this to be
        # set_value
        pandas, codeCompHelper = make_pandas(pandas_tree, query_file, args, treeHelp, output_name=set_value)
    else:
        # Let's try and write some pandas code from this
        pandas, codeCompHelper = make_pandas(pandas_tree, query_file, args, treeHelp)
    
    # We have created the pandas code, now let's write it out
    # Write out the pandas code, line by line
    if args.benchmarking:
        # We need a special mode for outputing
        with open(python_output_name, 'a+') as f:
            for line in pandas:
                f.write("    "+f"{line}\n")
        # Store relations
        relations_subqueries += codeCompHelper.relations
    else:        
        with open(python_output_name, 'a+') as f:
            for line in pandas:
                f.write(f"{line}\n")


def main():
    parser = init_argparse()
    args = parser.parse_args()
    
    if len(sys.argv)==1:
        # display help message when no args are passed.
        parser.print_help()
        sys.exit(1)
        
    # Set the Arguments
    query_file = args.file
    
    # Append the EXPLAIN options to the start of the file
    explain_opts = "EXPLAIN (COSTS FALSE, VERBOSE TRUE, FORMAT JSON) "
    
    # PSQL commands
    psql_file_command = "psql -d tpchdb -U tpch -a -f "
    psql_string_command = "psql -d tpchdb -U tpch -a -c '"

    # Create a folder for files and diagrams
    query_name = str(query_file.split("/")[-1]).split(".")[0]

    # Delete if already exists
    folder_path = Path("results" + "/" + query_name)
    if folder_path.exists() and folder_path.is_dir():
        shutil.rmtree(folder_path)
    # Make a folder
    Path(folder_path).mkdir(parents=True, exist_ok=True)
    # Move query_file into it
    shutil.copy(query_file, folder_path)
    
    # Handle multiple separate queries in one file
    query_file_data = None
    with open(query_file, 'r') as f:
        query_file_data = f.read()
    
    split_query = query_file_data.split(";")
    
    # Store relations from across subqueries, use as function parameter
    relations_subqueries = []
    # Store view names
    view_names = []
    
    # Create the filename
    python_output_name = args.output_location + "/" + args.name
    
    # Iterate through every subquery
    for i, sub_query in enumerate(split_query):
        
        # Skip iterations
        cleaned_sub_q = sub_query.strip()
        if sub_query == "":
            # Skip iteration if empty
            continue
        elif cleaned_sub_q[:4] == "drop":
            cleaned_sub_q = cleaned_sub_q + ";"
            command = psql_string_command + cleaned_sub_q + "'"

            cmd = subprocess.run(shlex.split(command), check=True, stdout=subprocess.DEVNULL)
            
            # Don't iterate after dropping
            continue

        # Automatically create explain_file from query_file
        explain_file = f"{folder_path}" + "/"+ query_name+"_explain_" + str(i) + ".sql"
        
        view_query = False
        # Determine whether this sub_query is a view or not
        if sub_query[:12] == "create view ":
            # Don't continue the execution if it's a create view
            view_name = str(str(sub_query.split("create view ")[1]).split("(")[0]).strip()
            view_names.append(view_name)
            
            # Exectute dropping this view
            command = psql_string_command + "drop view " + view_name + ";'"

            result = subprocess.run(shlex.split(command), stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)       
            captureError = str(result.stderr.decode('UTF-8')).strip()
            
            # Check if the error relates to the view not existing, fine, or something else!
            if captureError == "":
                # No error 
                pass
            elif captureError == 'ERROR:  view "' + view_name + '" does not exist' :
                # This error is okay
                pass
            else:
                raise ValueError(captureError)
            
            continue
            
            view_query = True
            # Store the view_name
            # Remove the create view stuff, split on as. Remove leading/trailing whitespace
            sub_query = str("".join(sub_query.split("as")[1:])).strip()
            # Add a semicolon on to the end of the sub_query
            sub_query = sub_query + ";"
        else:
            # Not a subquery
            if i != 0:
                # There has been a view, therefore use the previous query
                sub_query = split_query[i-1].strip() + ";" + "\n\n" + explain_opts + "\n" + sub_query.strip() + ";"
        
        # Write subquery into explain file
        with open(explain_file, "w") as writer:
            writer.write(sub_query)
            
        # Write the explain options out to the file
        # We already do this we have created a view beforehand
        if view_query == True:
            line_prepender(explain_file, explain_opts)
            
        # Check if no views then do line_prepender
        if len(split_query) == 2 and split_query[1] == "":
            # We have no view, so have to prepend the explain options
            line_prepender(explain_file, explain_opts)

        output_file = f"{folder_path}" + "/"+query_name+"_explain_" + str(i) + ".json"
        tree_output = f"{folder_path}" + "/"+query_name+"_explain_tree_" + str(i)
        tree_prune_output = f"{folder_path}" + "/"+query_name+"_explain_post_prune_tree_" + str(i)
        tree_pandas_output = f"{folder_path}" + "/"+ query_name+"_pandas_tree_" + str(i)
        command = psql_file_command + explain_file

        from clean_up_json import run
        # Execute the command, get the json and clean it
        run(command, output_file)

        # Load json from written file
        explain_json = ""
        with open(output_file) as f:
            # returns JSON object as a dictionary
            explain_json = json.load(f)[0]

        # Print out the json
        # print(json.dumps(explain_json, indent=4))

        from explain_tree import make_tree
        # Build a class structure that is nested within each other
        explain_tree = None
        explain_tree = make_tree(explain_json, explain_tree)

        # Let's try and visualise the explain tree now
        from visualising_tree import plot_tree
        if not args.benchmarking:
            plot_tree(explain_tree, tree_output)

        # Prune and alter the tree, for later use
        from explain_tree import solve_nested_loop_node, solve_prune_node, solve_aliases, solve_initplan_nodes, solve_view_set_values
        # We bump off hash nodes, we also need to do this with materialise nodes
        remove_nodes = ["Hash", "Materialize"]
        for node in remove_nodes:
            solve_prune_node(node, explain_tree)
            
        # Handle InitPlan Branches in queries
        output_trees = solve_initplan_nodes(explain_tree)
        
        # From this point on, we may have multiple output_trees
        for i in range(len(output_trees)):
            if isinstance(output_trees[i], tuple):
                # It's a tuple, so we use the first element
                solve_nested_loop_node(output_trees[i][0])
                solve_aliases(output_trees[i][0])
            else:
                # Not a tuple, just use the current element
                solve_nested_loop_node(output_trees[i])
                solve_aliases(output_trees[i])
                
        # Handle changes need to be made because of view_set_values
        solve_view_set_values(output_trees)
        
        # If output_trees is only 1, set it back in, no list
        if len(output_trees) == 1:
            output_trees = output_trees[0]

        # Plot tree after pruning/altering, show changes in tree
        if not args.benchmarking:
            if isinstance(output_trees, list):
                for i in range(len(output_trees)):
                    # Might be a tuple or not, depending on whether it's a subquery
                    if isinstance(output_trees[i], tuple):
                        # The 0th element is the tree
                        plot_tree(output_trees[i][0], tree_prune_output+"_"+str(i))
                    else:
                        plot_tree(output_trees[i], tree_prune_output+"_"+str(i))
                        
            else:
                plot_tree(output_trees, tree_prune_output)
                
        # Create a treeHelper once for all the plans
        from pandas_tree_to_pandas import TreeHelper
        overall_tree_helper = TreeHelper()

        # Iterate through all of the plans
        if isinstance(output_trees, list):
            for i in range(len(output_trees)):
                # Might be a tuple or not, depending on whether it's a subquery
                if isinstance(output_trees[i], tuple):
                    do_main_pandas_compilation(python_output_name, tree_pandas_output, query_file, args, output_trees[i][0], relations_subqueries, overall_tree_helper, set_value=output_trees[i][1], output_index=i)
                else:
                    do_main_pandas_compilation(python_output_name, tree_pandas_output, query_file, args, output_trees[i], relations_subqueries, overall_tree_helper, output_index=i)
            
            # Reset overall_tree_helper dict of node_id_tracker to be empty
            # But we maintain the totals in node_type_tracker, this stops us having overlapping names
            overall_tree_helper.node_id_tracker = {}
        else:
            # Just run it once
            do_main_pandas_compilation(python_output_name, tree_pandas_output, query_file, args, output_trees, relations_subqueries, overall_tree_helper)
                    
    # Write at the start of the file, the import and function definition
    if args.benchmarking:
        # Remove Duplicates
        relations_subqueries = list(dict.fromkeys(relations_subqueries))
        # Remove view names from this
        cleaned_relations_subqueries = [x for x in relations_subqueries if x not in view_names]
        
        line_prepender(python_output_name, "def query(" + str(cleaned_relations_subqueries)[1:-1].replace("'", "") + "):\n")
        line_prepender(python_output_name, "import pandas as pd\n")
        
    # Tear Down
    # If it's benchmarking, delete results
    if args.benchmarking:
        results_folder = Path("results")
        if results_folder.exists() and results_folder.is_dir():
            shutil.rmtree(results_folder)
            
if __name__ == "__main__":
    main()
