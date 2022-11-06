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
    return parser


def main():
    parser = init_argparse()
    args = parser.parse_args()
    
    if len(sys.argv)==1:
        # display help message when no args are passed.
        parser.print_help()
        sys.exit(1)
    
    # Set the Arguments
    query_file = args.file
    timing=args.benchmarking

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

    # Automatically create explain_file from query_file
    explain_file = f"{folder_path}" + "/"+ query_name+"_explain.sql"
    # Create a copy of the query
    shutil.copy(query_file,explain_file)
    # Append the EXPLAIN options to the start of the file
    def line_prepender(filename, line):
        with open(filename, 'r+') as f:
            content = f.read()
            f.seek(0, 0)
            f.write(line.rstrip('\r\n') + '\n' + content)

    explain_opts = "EXPLAIN (COSTS FALSE, VERBOSE TRUE, FORMAT JSON) "

    line_prepender(explain_file, explain_opts)

    output_file = f"{folder_path}" + "/"+query_name+"_explain.json"
    tree_output = f"{folder_path}" + "/"+query_name+"_explain_tree"
    tree_prune_output = f"{folder_path}" + "/"+query_name+"_explain_post_prune_tree"
    tree_pandas_output = f"{folder_path}" + "/"+ query_name+"_pandas_tree"
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
    if not args.benchmarking:
        plot_tree(explain_tree, tree_output)

    # Prune and alter the tree, for later use
    from explain_tree import solve_nested_loop_node, solve_hash_node
    solve_nested_loop_node(explain_tree)
    solve_hash_node(explain_tree)

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
    pandas = make_pandas(pandas_tree, query_file, timing)

    # Write out the pandas code, line by line
    with open(args.output_location + "/" + args.name, 'w') as f:
        for line in pandas:
            f.write(f"{line}\n")
    
    # Tear Down
    # If it's benchmarking, delete results
    if args.benchmarking:
        results_folder = Path("results")
        if results_folder.exists() and results_folder.is_dir():
            shutil.rmtree(results_folder)
            
if __name__ == "__main__":
    main()
