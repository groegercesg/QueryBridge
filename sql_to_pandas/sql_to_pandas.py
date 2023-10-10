import json
import shutil
import sys
# Add the parent directory of package to sys.path before attempting to import anything from package using absolute imports:
from pathlib import Path # if you haven't already done so
current_file = Path(__file__).resolve()
parent, root = current_file.parent, current_file.parents[1]
sys.path.append(str(root))
import argparse
import ast, traceback

# Prepare database file
from prepare_databases.prepare_duckdb import PrepareDuckDB
from prepare_databases.prepare_postgres import PreparePostgres
from prepare_databases.prepare_hyperdb import PrepareHyperDB
from prepare_databases.prepare_database import EXPLAIN_LOCATION

from hyper_parsing import generate_unparse_pandas_from_explain_and_query

def syntax_check_code(path):
    with open(path) as f:
        source = f.read()

    valid = True
    detail = ""

    try:
        ast.parse(source)
    except SyntaxError:
        valid = False
        detail = traceback.format_exc()
    
    return valid, detail

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

    parser.add_argument('--query_planner',
                    metavar='query_planner',
                    type=str,
                    required=True,
                    help='What query planner we would like to use, ')
    
    parser.add_argument('--planner_file',
                    metavar='planner_file',
                    type=str,
                    required=False,
                    default=None,
                    help='The file containing details for how and which planner to connect to.')
    
    parser.add_argument('--use_numpy',
                    metavar='use_numpy',
                    type=str2bool,
                    required=False,
                    help='Should we use numpy in our queries, this has been should to have a significant performance benefit')

    parser.add_argument('--groupby_sort_fusion',
                        metavar='groupby_sort_fusion',
                        type=str2bool,
                        required=False,
                        default=False,
                        help='Should we fuse groupby and sort operations. Defaults to False.')

    parser.add_argument('--merge_join_sort_fusion',
                        metavar='merge_join_sort_fusion',
                        type=str2bool,
                        required=False,
                        default=False,
                        help='Should we fuse merge join and sort operations. Defaults to False.')

    return parser

def line_prepender(filename, line):
    with open(filename, 'r+') as f:
        content = f.read()
        f.seek(0, 0)
        f.write(line.rstrip('\r\n') + '\n' + content)
        
def do_main_pandas_compilation(python_output_name, tree_pandas_output, query_file, args, tree, relations_subqueries, treeHelp, last_tree, set_value=None, output_index=None):
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
        # If We have last tree set, then we pass this through as the output_name
        if last_tree == True:
            # Let's try and write some pandas code from this
            pandas, codeCompHelper = make_pandas(pandas_tree, query_file, args, treeHelp, output_name="RETURN")
        else:
            pandas, codeCompHelper = make_pandas(pandas_tree, query_file, args, treeHelp)
    
    # We have created the pandas code, now let's write it out
    # Write out the pandas code, line by line
    if args.benchmarking:
        # We need a special mode for outputing
        with open(python_output_name, 'a+') as f:
            for line in pandas:
                f.write("    "+f"{line}\n")
        # Store relations, but only ones that aren't in aliasRelationPairs.keys
        remove = []
        ccRelations = list(codeCompHelper.relations)
        for i in range(len(ccRelations)):
            if ccRelations[i] in codeCompHelper.aliasRelationPairs:
                remove.append(i)
                
        # Reverse remove
        remove.reverse()
        # Do the deletes
        for num in remove:
            del ccRelations[num]
        
        relations_subqueries += ccRelations
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

    # Validate python version
    if (int(sys.version_info[0]) != 3):
        print("Python Version was not 3.X, exiting!")
        exit(0)
        
    # Validate arguments
    if args.query_planner == "Postgres":
        print("Planning the query with: " + str(args.query_planner))
    elif args.query_planner == "Duck_DB":
        print("Planning the query with: " + str(args.query_planner))
    elif args.query_planner == "Hyper_DB":
        print("Planning the query with: " + str(args.query_planner))
    else:
        print("Unrecognised Query Planning option: " + str(args.query_planner))
        print("The current supported options are: ")
        print("\t 'Postgres'")
        print("\t 'Duck_DB'")
        exit(0)
        
    # Set the Arguments
    query_file = args.file
    
    # Create a folder for files and diagrams
    query_name = str(query_file.split("/")[-1]).split(".")[0]

    # Delete if already exists
    folder_path = Path(args.output_location)
    if folder_path.exists() and folder_path.is_dir():
        try:
            shutil.rmtree(folder_path)
        except:
            # Sometimes due to AFS permission we're not able to delete the directory
            pass
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
    
    # Variable to store the explain content
    explain_content = []
    
    # Postgres planning
    if args.query_planner == "Postgres":
        # Validation for db_file
        if args.planner_file == None:
            raise Exception("No database connection file specified")
        
        db = PreparePostgres(args.planner_file)
    elif args.query_planner == "Duck_DB":
        # Validation for duck_db_prep
        if args.planner_file == None:
            raise Exception("No duck db preparation file specified")
        
        db = PrepareDuckDB(args.planner_file)
    elif args.query_planner == "Hyper_DB":
        # Validation for duck_db_prep
        if args.planner_file == None:
            raise Exception("No duck db preparation file specified")
        
        db = PrepareHyperDB(args.planner_file)
    else:
        raise Exception(f"Unrecognised option for query planner: {args.query_planner}")
    
    # Iterate through every subquery
    for i, sub_query in enumerate(split_query):
        
        # Skip iterations
        cleaned_sub_q = sub_query.strip()
        if (sub_query == "") or (sub_query == "\n"):
            # Skip iteration if empty
            continue
        elif cleaned_sub_q[:4] == "drop":

            # Drop command with the DB connection object
            db.execute_query(cleaned_sub_q + ";")

            # Don't iterate after dropping
            continue

        view_query = False
        # Determine whether this sub_query is a view or not
        if "create view " in sub_query:
            # Don't continue the execution if it's a create view
            view_name = str(str(sub_query.split("create view ")[1]).split("(")[0]).strip()
            view_names.append(view_name)
            
            # Execute dropping this view
            if args.query_planner == "Postgres":
                db.execute_query("drop view " + view_name + ";")
            
            continue
        else:
            # Not a subquery
            if i != 0:
                # There has been a view, therefore use the previous query
                old_subquery = sub_query
                sub_query = [split_query[i-1].strip() + ";"]
                sub_query.append(EXPLAIN_LOCATION) 
                sub_query.append(old_subquery.strip() + ";")
        
        
        # Write subquery into explain the explain_content variable  
        if isinstance(sub_query, str):
            if sub_query[-1] != ";":
                sub_query = sub_query + ";"
            explain_content.append(sub_query)
        elif isinstance(sub_query, list):
            explain_content.extend(sub_query)
        else:
            raise Exception(f"Unknown format for the variable 'sub_query' it is: {type(sub_query)}")
            
        # Write the explain options out to the file
        # We already do this we have created a view beforehand
        if view_query == True:
            explain_content = [EXPLAIN_LOCATION]  + explain_content
            
        # Check if no views then do line_prepender
        if len(split_query) == 2 and ((split_query[1] == "") or (split_query[1] == '\n')):
            # We have no view, so have to prepend the explain options
            explain_content = [EXPLAIN_LOCATION] + explain_content
        elif len(split_query) == 1:
            explain_content = [EXPLAIN_LOCATION] + explain_content
            
        explain_output_path = f"{folder_path}" + "/"+ query_name+"_explain_" + str(i) + ".json"
        tree_output = f"{folder_path}" + "/"+query_name+"_explain_tree_" + str(i)
        tree_prune_output = f"{folder_path}" + "/" +query_name+"_explain_post_prune_tree_" + str(i)
        tree_pandas_output = f"{folder_path}" + "/" + query_name+"_pandas_tree_" + str(i)
        expr_tree_output = f"{folder_path}" + "/" + query_name+ "_expression_tree_"
        
        if db.is_database_empty() == True:
            # Database is empty
            raise Exception("The database is empty, please specify a connection to a database with tables")
        
        # We can request the explain data from the database
        
        explain_json, explain_content = db.get_explain(explain_content, query_name)

        # Write out explain_content to explain_file_path
        with open(explain_output_path, "w") as outfile:
            # returns JSON object as a dictionary, write that to outfile
            outfile.write(json.dumps(explain_json, indent=4))

        # Print out the json
        # print(json.dumps(explain_content, indent=4))

        from explain_tree import make_tree_from_pg, make_tree_from_duck
        # Build a class structure that is nested within each other
        explain_tree = None
        if args.query_planner == "Postgres":
            explain_tree = make_tree_from_pg(explain_json, explain_tree)
        elif args.query_planner == "Duck_DB":
            if isinstance(sub_query, list):
                last_query = sub_query[-1]
            elif isinstance(sub_query, str):
                last_query = sub_query
            else:
                raise Exception(f'Unable to assign sub_query to last_query, it was of an odd type: {type(sub_query)}')
            explain_tree = make_tree_from_duck(explain_json, explain_tree, last_query)   
        elif args.query_planner == "Hyper_DB":
            unparse_pandas = generate_unparse_pandas_from_explain_and_query(explain_json, query_file)
            
            with open(python_output_name, 'w') as fp:
                for line in unparse_pandas.getPandasContent():
                    if "\n" in line:
                        writeContent = line.split("\n")
                    else:
                        writeContent = [line]
                        
                    for line in writeContent:
                        # write each item on a new line
                        potentialTab = ""
                        if args.benchmarking:
                            potentialTab = "    "
                        fp.write(f"{potentialTab}{line}\n")
                        
                # Write return 'blah', if benchmarking
                if args.benchmarking:
                    fp.write(f"{potentialTab}return {unparse_pandas.pandas_tree.tableName}\n")
            
            relations_subqueries = unparse_pandas.relations
            
            continue
        else:
            raise Exception("Unknown database planner specified")
        
        # Let's try and visualise the explain tree now
        from visualising_tree import plot_tree
        if not args.benchmarking:
            plot_tree(explain_tree, tree_output)

        # Prune and alter the tree, for later use
        from explain_tree import solve_nested_loop_node, solve_prune_node, solve_null_output, solve_initplan_nodes, solve_view_set_values, solve_groupby_fusion, solve_merge_join_fusion
        # We bump off hash nodes, we also need to do this with materialise nodes
        remove_nodes = ["Hash", "Materialize"]
        for node in remove_nodes:
            solve_prune_node(node, explain_tree)
            
        # Clear "NULL" from output
        solve_null_output(explain_tree)
        
        # Perform the groupby and merge join fusion
        if args.groupby_sort_fusion == True:
            solve_groupby_fusion(explain_tree)
        
        if args.merge_join_sort_fusion == True:
            solve_merge_join_fusion(explain_tree)

        # Handle InitPlan Branches in queries
        output_trees = solve_initplan_nodes(explain_tree)
        
        # From this point on, we may have multiple output_trees
        for i in range(len(output_trees)):
            if isinstance(output_trees[i], tuple):
                # It's a tuple, so we use the first element
                #solve_aliases(output_trees[i][0])
                solve_nested_loop_node(output_trees[i][0])
            else:
                # Not a tuple, just use the current element
                #solve_aliases(output_trees[i])
                solve_nested_loop_node(output_trees[i])
                
        # TODO: This may not make all the replacements that we actually need it to
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
        
        local_use_numpy = None
        if hasattr(args, "use_numpy"):
            if args.use_numpy != None:
                # We have set use_numpy in args, use that false
                local_use_numpy = args.use_numpy
            else:
                # Otherwise, we haven't, set as False
                local_use_numpy = False
        else:
            # Otherwise, we haven't, set as False
            local_use_numpy = False
        from pandas_tree_to_pandas import TreeHelper
        overall_tree_helper = TreeHelper(expr_tree_output, args.benchmarking, local_use_numpy, args.groupby_sort_fusion, args.merge_join_sort_fusion)

        # Iterate through all of the plans
        if isinstance(output_trees, list):
            for i in range(len(output_trees)):
                # Check if is last tree
                if i == (len(output_trees) - 1):
                    last_tree = True
                else:
                    last_tree = False
                
                # Might be a tuple or not, depending on whether it's a subquery
                if isinstance(output_trees[i], tuple):
                    do_main_pandas_compilation(python_output_name, tree_pandas_output, query_file, args, output_trees[i][0], relations_subqueries, overall_tree_helper, last_tree, set_value=output_trees[i][1], output_index=i)
                else:
                    do_main_pandas_compilation(python_output_name, tree_pandas_output, query_file, args, output_trees[i], relations_subqueries, overall_tree_helper, last_tree, output_index=i)
            
                # Reset overall_tree_helper dict of node_id_tracker to be empty
                # But we maintain the totals in node_type_tracker, this stops us having overlapping names
                overall_tree_helper.node_id_tracker = {}
        else:
            # Only one tree, set last tree to be true
            last_tree = True
            # Just run it once
            do_main_pandas_compilation(python_output_name, tree_pandas_output, query_file, args, output_trees, relations_subqueries, overall_tree_helper, last_tree)
                    
    # Write at the start of the file, the import and function definition
    if args.benchmarking:
        # Remove Duplicates
        relations_subqueries = list(dict.fromkeys(relations_subqueries))
        # Remove view names from this
        cleaned_relations_subqueries = [x for x in relations_subqueries if x not in view_names]
        
        line_prepender(python_output_name, "def q" + str(query_name) + "(" + str(cleaned_relations_subqueries)[1:-1].replace("'", "") + "):\n")
        line_prepender(python_output_name, "import pandas as pd\n")
        
        if hasattr(args, "use_numpy"):
            if args.use_numpy != None and args.use_numpy == True:
                # We have set use_numpy in args, use that false
                line_prepender(python_output_name, "import numpy as np\n")

        # Tear Down, delete results
        results_folder = Path("results")
        if results_folder.exists() and results_folder.is_dir():
            shutil.rmtree(results_folder)

    # After writing, check the syntax
    decision, detail = syntax_check_code(python_output_name)
    if decision != True:
        print("Syntax Error detected in Generated Python Code: " + str(python_output_name))
        print(detail)
        exit(0)
            
if __name__ == "__main__":
    main()
