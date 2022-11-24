import json
import shutil
import sys
from pathlib import Path
import argparse

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

    
    return parser


def line_prepender(filename, line):
    with open(filename, 'r+') as f:
        content = f.read()
        f.seek(0, 0)
        f.write(line.rstrip('\r\n') + '\n' + content)


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
    
    # Create the filename
    python_output_name = args.output_location + "/" + args.name
    
    # Iterate through every subquery
    for i, sub_query in enumerate(split_query):
        
        # Skip iterations
        if sub_query == "":
            # Skip iteration if empty
            continue

        # Automatically create explain_file from query_file
        explain_file = f"{folder_path}" + "/"+ query_name+"_explain_" + str(i) + ".sql"
        
        view_query = False
        # Determine whether this sub_query is a view or not
        if sub_query[:12] == "create view ":
            view_query = True
            # Store the view_name
            view_name = str(str(sub_query.split("create view ")[1]).split("(")[0]).strip()
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
        command = "psql -d tpchdb -U tpch -a -f " + explain_file

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
        from visualising_tree import plot_tree, plot_pandas_tree
        if not args.benchmarking:
            plot_tree(explain_tree, tree_output)

        # Prune and alter the tree, for later use
        from explain_tree import solve_nested_loop_node, solve_prune_node, solve_aliases
        solve_nested_loop_node(explain_tree)
        # We bump off hash nodes, we may also need to do this with materialise nodes
        solve_prune_node("Hash", explain_tree)
        solve_prune_node("Materialize", explain_tree)
        solve_aliases(explain_tree)

        # Plot tree after pruning/altering, show changes in tree
        if not args.benchmarking:
            plot_tree(explain_tree, tree_prune_output)

        # Let's try create a pandas tree
        from pandas_tree import make_pandas_tree
        pandas_tree = make_pandas_tree(explain_tree, query_file)

        # Make tree of pandas!
        if not args.benchmarking:
            plot_pandas_tree(pandas_tree, tree_pandas_output)

        # Let's try and write some pandas code from this
        from pandas_tree_to_pandas import make_pandas
        pandas, codeCompHelper = make_pandas(pandas_tree, query_file, args.column_ordering)

        # Write out the pandas code, line by line
        if args.benchmarking:
            # We need a special mode for outputing
            with open(python_output_name, 'w') as f:
                for line in pandas:
                    f.write("    "+f"{line}\n")
            # Store relations
            relations_subqueries += codeCompHelper.relations
        else:        
            with open(python_output_name, 'a') as f:
                for line in pandas:
                    f.write(f"{line}\n")
                    
    # Write at the start of the file, the import and function definition
    if args.benchmarking:
        line_prepender(python_output_name, "def query(" + str(relations_subqueries)[1:-1].replace("'", "") + "):\n")
        line_prepender(python_output_name, "import pandas as pd\n")
        
    # Tear Down
    # If it's benchmarking, delete results
    #if args.benchmarking:
    #    results_folder = Path("results")
    #    if results_folder.exists() and results_folder.is_dir():
    #        shutil.rmtree(results_folder)
            
if __name__ == "__main__":
    main()
