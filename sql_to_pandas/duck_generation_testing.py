import sys
# Add the parent directory of package to sys.path before attempting to import anything from package using absolute imports:
from pathlib import Path # if you haven't already done so
current_file = Path(__file__).resolve()
parent, root = current_file.parent, current_file.parents[1]
sys.path.append(str(root))

from tpch_helpers import *
from sdqlpy_optimisations import sdqlpy_apply_optimisations

from proof_of_concept_duckdb_queries import *

from uplan_parsing import uplan_to_exec_format
from duck_transformation import duck_to_uplan

def convert_duck_to_x(desired_format):
    table_schema = configure_table_schema({})
    
    uplan_opts = ""
    # uplan_opts = ["ColumnElimination"]
    duck_tree = Query3()
    
    uplan_tree = duck_to_uplan(
        duck_tree
    )
    
    query_name = "query6"
    
    unparse_content = uplan_to_exec_format(
        uplan_tree,
        desired_format,
        table_schema,
        uplan_opts,
        query_name
    )
    
    print(unparse_content)
    content_size = 0
    if desired_format == "pandas":
        pandas_content = unparse_content.getPandasContent()
        print(pandas_content)
        content_size = len(pandas_content)
    elif desired_format == "sdqlpy":
        # Do Optimisations
        # With Pipeline Breakers
        # unparse_content.sdqlpy_tree = sdqlpy_apply_optimisations(unparse_content.sdqlpy_tree, ["UpdateSum", "VerticalFolding", "Dense", "PipelineBreaker"], "0.9") # 
        # From Naive
        unparse_content.sdqlpy_tree = sdqlpy_apply_optimisations(unparse_content.sdqlpy_tree, ["UpdateSum", "VerticalFolding", "PipelineBreaker"], "0.9") # 
        
        content_size = len(unparse_content.getSDQLpyContent())
    else:
        raise Exception("Unrecognised desired format")
    
    assert content_size > 0
    print(f"Generated {content_size} lines of SDQLpy code")

# generate_hyperdb_explains()
# # inspect_explain_plans()
# # parse_explain_plans()
convert_duck_to_x("pandas") # sdqlpy || pandas
