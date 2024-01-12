# Use the lark package to parse strings from the Output into a tree-based class representation
import sys
# Add the parent directory of package to sys.path before attempting to import anything from package using absolute imports:
from pathlib import Path # if you haven't already done so
current_file = Path(__file__).resolve()
parent, root = current_file.parent, current_file.parents[1]
sys.path.append(str(root))
from prepare_databases.prepare_hyperdb import PrepareHyperDB
from os import listdir
from os.path import isfile, join
import json

from hyper_classes import *

from sdqlpy_unparser import *
from sdqlpy_transformer import *
from sdqlpy_optimisations import *

def generate_hyperdb_explains():
    db = PrepareHyperDB('hyperdb_tpch.hyper')
    db.prepare_database('data_storage')
    query_directory = 'sql_to_pandas/tpch_no_limit_order'
    explain_directory = 'sql_to_pandas/hyperdb_tpch_explain_no_limit_order'
    
    onlyfiles = [f for f in listdir(query_directory) if isfile(join(query_directory, f))]
    
    for query_file in onlyfiles:
        with open(f'{query_directory}/{query_file}') as r:
            explain_content = r.read()
            
        query_name = query_file.split('.')[0]
        
        # TODO: Skip Query 15:
        # HyperException: Views are disabled.
        if str(query_name) == "15":
            continue
    
        # Else, we can request the explain data from the database
        explain_json, query_content = db.get_explain(explain_content, query_name)

        # Write out explain_content to explain_file_path
        with open(f'{explain_directory}/{query_name}_hyper.json', "w") as outfile:
            # returns JSON object as a dictionary, write that to outfile
            outfile.write(json.dumps(explain_json, indent=4))
            
        print(f'Generated explain output for: {query_file}')
        
    print(f'Generated Explain output for all {len(onlyfiles)} queries.')

def gather_operators(explain_node):
    operators = set()
    has_below = False
    for option in ["input", "left", "right"]: 
        if option in explain_node:
            has_below = True
            operators.update(gather_operators(explain_node[option]))
    
    if has_below == False and explain_node["operator"] != "tablescan":
        raise Exception(f"New leaf node discovered: { explain_node['operator'] }")
    
    operators.add(explain_node["operator"])
    return operators

def inspect_explain_plans():
    explain_directory = 'sql_to_pandas/hyperdb_tpch_explain_no_limit_order'
    
    onlyfiles = [f for f in listdir(explain_directory) if isfile(join(explain_directory, f))]
    
    operators = set()
    
    for explain_file in onlyfiles:
        with open(f'{explain_directory}/{explain_file}') as r:
            explain_content = json.loads(r.read())

        operators.update(gather_operators(explain_content))
    
    print("Below are the operators that Hyper DB Uses:")
    print(operators)
    
def fix_explictScanNodes(op_node: HyperBaseNode, all_nodes: dict):
    # Visit Children
    if isinstance(op_node, JoinBaseNode) and op_node.isJoinNode == True:
        fix_explictScanNodes(op_node.left, all_nodes)
        fix_explictScanNodes(op_node.right, all_nodes)
    elif op_node.child != None:
        fix_explictScanNodes(op_node.child, all_nodes)
    else:
        # A leaf node
        pass
    
    # Children visited, now process current node
    if isinstance(op_node, explicitscanNode):
        if op_node.isRetrieve == True and op_node.child == None:
            assert op_node.targetOperator in all_nodes
            # Deepcopy it?
            op_node.child = copy.deepcopy(all_nodes[op_node.targetOperator])

def create_hyper_operator_tree(explain_json, all_nodes: dict):
    operator_name = explain_json["operator"].lower()
    if operator_name not in SUPPORTED_HYPER_OPERATORS:
        raise Exception(f"Unknown operator in explain json: {operator_name}")
    
    operator_class = None
    if operator_name == "executiontarget":
        operator_class = executiontargetNode(explain_json["output"], explain_json["outputNames"])
    elif operator_name == "groupby":
        keyExpressions = []
        if "keyExpressions" in explain_json:
            keyExpressions = explain_json["keyExpressions"]
        aggExpressions = []
        if "aggExpressions" in explain_json:
            aggExpressions = explain_json["aggExpressions"]
        operator_class = groupbyNode(keyExpressions, aggExpressions, explain_json["aggregates"])
    elif operator_name == "tablescan":
        tableRestrictions = []
        tableFilters = []
        if "restrictions" in explain_json:
            assert isinstance(explain_json["restrictions"], list)
            tableRestrictions = explain_json["restrictions"]
        if "residuals" in explain_json:
            assert isinstance(explain_json["residuals"], list)
            tableFilters = explain_json["residuals"]
        operator_class = tablescanNode(explain_json["debugName"]["value"], explain_json["values"], tableRestrictions, tableFilters)
    elif operator_name == "sort":
        limitValue = None
        if "limit" in explain_json:
            limitValue = explain_json["limit"]
        operator_class = sortNode(explain_json["criterion"], limitValue)
    elif operator_name == "map":
        operator_class = mapNode(explain_json["values"])
    elif operator_name == "select":
        operator_class = selectNode(explain_json["condition"])
    elif operator_name == "groupjoin":
        operator_class = groupjoinNode(explain_json["semantic"],
                                       explain_json["leftKey"],
                                       explain_json["rightKey"],
                                       explain_json["leftExpressions"],
                                       explain_json["leftAggregates"],
                                       explain_json["rightExpressions"],
                                       explain_json["rightAggregates"])
    elif operator_name == "join":
        operator_class = joinNode("inner", explain_json["method"], explain_json["condition"])
    elif operator_name in ["leftsemijoin", "leftantijoin", "rightsemijoin", "rightantijoin"]:
        operator_class = joinNode(operator_name, explain_json["method"], explain_json["condition"])
    elif operator_name == "leftsinglejoin":
        # Remap a left singlejoin to an inner join
        operator_class = joinNode("inner", explain_json["method"], explain_json["condition"])
    elif operator_name == "explicitscan":
        operator_class = explicitscanNode(explain_json["mapping"])
    else:
        raise Exception(f"No Operator Class Creation has been defined for the {operator_name} operator.")

    # Validate children operator options are as expected
    if ("input" in explain_json) and (any(x in explain_json for x in ["left", "right"])):
        raise Exception("There shouldn't be 'input' as well as either 'left'/'right' parameters for an operator")
       
    # Add children to the different operators
    if ("input" in explain_json):
        if isinstance(explain_json["input"], int):
            assert isinstance(operator_class, explicitscanNode)
            operator_class.setRetrieve(True, explain_json["input"])
        else:
            operator_class.addChild(create_hyper_operator_tree(explain_json["input"], all_nodes))
    elif any(x in explain_json for x in ["left", "right"]):
        # It should be an operator type if we're here
        assert isinstance(operator_class, JoinBaseNode) and hasattr(operator_class, "isJoinNode") and operator_class.isJoinNode == True
        operator_class.addLeft(create_hyper_operator_tree(explain_json["left"], all_nodes))
        operator_class.addRight(create_hyper_operator_tree(explain_json["right"], all_nodes))
    else:
        # Only table scan node should have no children
        assert isinstance(operator_class, tablescanNode)
        
    # Set Hyper ID
    operatorID = explain_json["operatorId"]
    assert isinstance(operatorID, int)
    operator_class.setHyperID(operatorID)
    # Set cardinality
    if "selectivity" in explain_json:
        #cardValue = int(float(explain_json["cardinality"]) / float(explain_json["selectivity"]))
        cardValue = int(explain_json["cardinality"])
    else: 
        cardValue = int(explain_json["cardinality"])
    operator_class.setCardinality(cardValue)
    # Add to all nodes
    assert operatorID not in all_nodes
    all_nodes[operatorID] = operator_class
    
    return operator_class

def generate_unparse_content_from_explain_and_query(explain_json, query_file, output_format, table_keys):
    query_name = query_file.split("/")[-1].split(".")[0].strip()
    
    all_nodes = dict()
    op_tree = create_hyper_operator_tree(explain_json, all_nodes)
    
    # Transform the operator_trees
    # Task 0: Fix tree and it's explicitScan (where retrieve)
    fix_explictScanNodes(op_tree, all_nodes)
    print(f"Transformed Explicit Scan Nodes in Plan {query_name} Hyper Tree")
    # Task 1: Solve the 'v1' references, in 'iu'
    transform_hyper_iu_references(op_tree)
    print(f"Transformed IU References in Plan {query_name} Hyper Tree")
    # Task 2: Convert Hyper DB Nodes into Universal Plan Nodes
    op_tree = transform_hyper_to_universal_plan(op_tree)
    print(f"Transformed Plan {query_name} Hyper Tree into Universal Plan")
    # Task 3: Fix sql table aliases in tablescans
    transform_sql_table_aliases(query_file, op_tree)
    
    # Test 1: top node should be OutputNode
    assert audit_universal_plan_tree_outputnode(op_tree)
    # Test 2: all leaf nodes should be ScanNode
    assert audit_universal_plan_tree_scannode(op_tree)
    
    unparse_content = None
    if output_format == "pandas":
        # Convert Universal Plan Tree to Pandas Tree
        op_tree = convert_universal_to_pandas(op_tree)
        print(f"Converted Universal Plan Tree of {query_name} into Pandas Tree")

        # Unparse Pandas Tree
        # try:
        unparse_content = UnparsePandasTree(op_tree)
        # except:
        #     print(f"Pandas Generation for Query '{query_name}' Failed.")
        #     raise Exception("Failed Pandas Generation")
    elif output_format == "sdqlpy":
        # Convert Universal Plan Tree to SDQLpy Tree
        sdqlpy_tree = convert_universal_to_sdqlpy(op_tree, table_keys)
        
        # Test: All leaf nodes should be SDQLpyRecordNode
        assert audit_sdqlpy_tree_leafnode(sdqlpy_tree)
        print(f"Converted Universal Plan Tree of {query_name} into SDQLpy Tree")
        
        # Unparse SDQLpy Tree
        # try:
        unparse_content = UnparseSDQLpyTree(sdqlpy_tree)
        # except:
        #     print(f"SDQLpy Generation for Query '{query_name}' Failed.")
        #     raise Exception("Failed SDQLpy Generation")
    else:
        raise Exception(f"Unexpected format for output_format: {output_format}")
        
    assert unparse_content != None
    return unparse_content

def convert_explain_plan_to_x(desired_format):
    query_directory = 'sql_to_pandas/tpch_no_limit_order'
    explain_directory = 'sql_to_pandas/hyperdb_tpch_explain_no_limit_order'
    
    # Ignore Query 15 for now
    query_files = [f for f in listdir(query_directory) if isfile(join(query_directory, f)) and str(f).split(".")[0] != "15"]
    explain_files = [f for f in listdir(explain_directory) if isfile(join(explain_directory, f))]
    
    combined_sql_content = list(zip(query_files, explain_files))
    
    if desired_format == "sdqlpy":
        db = PrepareHyperDB('hyperdb_tpch.hyper')
        db.prepare_database('data_storage')
        table_keys = db.get_table_keys()
    else:
        table_keys = None
    
    supported_queries = ["1", "2", "3" ,"4", "5", "6", "8", "9", "10", "11", "12", "14", "15_cte", "16", "18", "19", "20"]
    
    print(f"We currently support {len(supported_queries)} out of a total of 22")
    
    for sql_file, explain_file in combined_sql_content:
        if sql_file.split(".")[0] not in supported_queries:
            continue
        
        with open(f'{explain_directory}/{explain_file}') as r:
            explain_content = json.loads(r.read())
            
            unparse_content = generate_unparse_content_from_explain_and_query(
                explain_content,
                f'{query_directory}/{sql_file}',
                desired_format,
                table_keys)
            
            print(unparse_content)
            content_size = 0
            if desired_format == "pandas":
                content_size = len(unparse_content.getPandasContent())
            elif desired_format == "sdqlpy":
                # Do Optimisations
                unparse_content.sdqlpy_tree = apply_optimisations(unparse_content.sdqlpy_tree, ["VerticalFolding"]) # , "PipelineBreaker"
                
                content_size = len(unparse_content.getSDQLpyContent())
            else:
                raise Exception("Unrecognised desired format")
            
            assert content_size > 0
            print(f"Generated {content_size} lines of SDQLpy code")
    
def parse_explain_plans():
    query_directory = 'sql_to_pandas/tpch_queries'
    explain_directory = 'sql_to_pandas/hyperdb_tpch_explain_no_limit_order'
    
    # Ignore Query 15 for now
    query_files = [f for f in listdir(query_directory) if isfile(join(query_directory, f)) and str(f).split(".")[0] != "15"]
    explain_files = [f for f in listdir(explain_directory) if isfile(join(explain_directory, f))]
    
    combined_sql_content = list(zip(query_files, explain_files))
    
    all_operator_trees = []
    for sql_file, explain_file in combined_sql_content:
        # if sql_file.split(".")[0] not in ["3"]:
        #    continue
         
        print(f"Transforming {explain_file} into a Hyper Tree")
        with open(f'{explain_directory}/{explain_file}') as r:
            explain_content = json.loads(r.read())
        
        # Gather all the nodes
        all_nodes = dict()
        op_tree = create_hyper_operator_tree(explain_content, all_nodes)
        all_operator_trees.append([sql_file, op_tree, all_nodes])

    #assert len(all_operator_trees) == 21
    print("All 21 HyperDB plans have been parsed into Hyper DB Class Trees")
    
    # Transform the operator_trees
    # Task 0: Fix tree and it's explicitScan (where retrieve)
    for tree in all_operator_trees:
        fix_explictScanNodes(tree[1], tree[2])
        print(f"Transformed Explicit Scan Nodes in Plan {tree[0]} Hyper Tree")
    # Task 1: Solve the 'v1' references, in 'iu'
        # These propagate up, and are occasionally reset by things like "mapNode" or "groupbyNode"
        # Postorder traversal is required for this, make a dict of the pairs:
            # v1: l_suppkey
            # v3: p_partkey
            # ...
        # Also in this pass we should parse expressions (and similar things) into ExpressionOperators
    for tree in all_operator_trees:
        transform_hyper_iu_references(tree[1])
        print(f"Transformed IU References in Plan {tree[0]} Hyper Tree")
    # Task 2: Convert Hyper DB Nodes into Universal Plan Nodes
    for tree in all_operator_trees:
        tree[1] = transform_hyper_to_universal_plan(tree[1])
        print(f"Transformed Plan {tree[0]} Hyper Tree into Universal Plan")
    # Task 3: Fix sql table aliases in tablescans
    for tree in all_operator_trees:
        transform_sql_table_aliases(f"{query_directory}/{tree[0]}", tree[1])
    
    print("Hyper Tree Plans have been Transformed into Universal Plan Trees")
    
    # Audit Universal Plan Trees
    for tree in all_operator_trees:
        # Test 1: top node should be OutputNode
        assert audit_universal_plan_tree_outputnode(tree[1])
        # Test 2: all leaf nodes should be ScanNode
        assert audit_universal_plan_tree_scannode(tree[1])
        
    print("Universal Plan Trees have been Audited")
    
    # Convert Universal Plan Tree to Pandas Tree
    for tree in all_operator_trees:
        tree[1] = convert_universal_to_pandas(tree[1])
        print(f"Converted Universal Plan Tree of {tree[0]} into Pandas Tree")

    print("Universal Plan Trees have been converted into Pandas Trees")

    # Transform Pandas Tree in ways that are required
    for tree in all_operator_trees:
        pass

    # Unparse Pandas Trees to list
    failed_counter = 0
    for tree in all_operator_trees:
        try:
            pandas_content = UnparsePandasTree(tree[1]).getPandasContent()
        except:
            print(f"Pandas Generation for Query '{tree[0]}' Failed.")
            failed_counter += 1
            
        # print(f"Pandas Content for Plan '{tree[0]}':")
        # for line in pandas_content:
        #     print(line)
        # print("-" * 15)
    
    if failed_counter > 0:
        print("-"*15)
        print(f"We failed {failed_counter} out of {len(all_operator_trees)}; or {round((failed_counter / len(all_operator_trees)) * 100, 2)}%.")
    else:
        print("-"*15)
        print(f"We succeeded in unparsing all {len(all_operator_trees)} Pandas trees into Pandas Content")
    
    print("Unparsed Pandas Tree(s) into Pandas Content")
    
from pandas_unparser_v2 import *
from universal_plan_nodes import *
from sqlglot import parse_one, exp

def transform_sql_table_aliases(sql_file_path: str, op_tree: HyperBaseNode):
    def get_table_aliases(sql_file: str) -> dict:
        with open(f'{sql_file_path}') as f:
            sql_content = f.read()
        
        table_aliases = dict()
        for table in parse_one(sql_content).find_all(exp.Table):
            if table.alias != '':
                table_aliases[table.alias] = table.name
        
        return table_aliases

    def walk_universal_tree(op_tree: UniversalBaseNode, aliases: dict):
        match op_tree:
            case BinaryBaseNode():
                walk_universal_tree(op_tree.left, aliases)
                walk_universal_tree(op_tree.right, aliases)
            case UnaryBaseNode():
                walk_universal_tree(op_tree.child, aliases)
            case UniversalBaseNode():
                if isinstance(op_tree, ScanNode):
                    # Check aliases
                    if op_tree.tableName in aliases:
                        op_tree.tableName = aliases[op_tree.tableName]
            case _:
                raise Exception(f"We are walking a universal plan tree, unknown type: {op_tree.__class__}") 
    
    aliases = get_table_aliases(sql_file_path)
    walk_universal_tree(op_tree, aliases)
    

def audit_universal_plan_tree_outputnode(op_tree: UniversalBaseNode) -> bool:
    return isinstance(op_tree, OutputNode)

def audit_universal_plan_tree_scannode(op_tree: UniversalBaseNode) -> bool:
    def get_leaf_nodes(op_tree: UniversalBaseNode) -> list[UniversalBaseNode]:
        leafs = []
        def _get_leaf_nodes(op_node: UniversalBaseNode):
            match op_node:
                case BinaryBaseNode():
                    _get_leaf_nodes(op_node.left)
                    _get_leaf_nodes(op_node.right)
                case UnaryBaseNode():
                    _get_leaf_nodes(op_node.child)
                case UniversalBaseNode():
                    leafs.append(op_node)
                case _:
                    raise Exception(f"We are auditing a universal plan tree, all nodes should be at minimum a UniversalBaseNode, not: {op_node.__class__}") 
        _get_leaf_nodes(op_tree)
        return leafs
    
    # Get all leaves, make sure they're all ScanNode
    all_leaves = get_leaf_nodes(op_tree)
    return all(isinstance(leaf, ScanNode) for leaf in all_leaves)

def transform_hyper_to_universal_plan(op_tree: HyperBaseNode) -> UniversalBaseNode:
    def equal_left_right_keys(left_keys: list[ExpressionBaseNode], right_keys: list[ExpressionBaseNode]) -> list[EqualsOperator]:
        assert len(left_keys) == len(right_keys)
        equate_keys = []
        for i in range(len(left_keys)):
            newEqualsOperator = EqualsOperator()
            newEqualsOperator.addLeft(
                left_keys[i]
            )
            newEqualsOperator.addRight(
                right_keys[i]
            )
            equate_keys.append(
                newEqualsOperator
            )
        # Parse into a single object
        if len(equate_keys) >= 2:
            equate_keys = join_statements_with_operator(equate_keys, "AndOperator")
        else:
            equate_keys = equate_keys[0]
        return equate_keys
    
    def visit_hyper_nodes(op_node: HyperBaseNode):
        # Visit Children
        if isinstance(op_node, JoinBaseNode) and op_node.isJoinNode == True:
            leftNode = visit_hyper_nodes(op_node.left)
            rightNode = visit_hyper_nodes(op_node.right)
        elif op_node.child != None:
            childNode = visit_hyper_nodes(op_node.child)
        else:
            # A leaf node
            pass
        
        # Create a 'new_op_node' from an existing 'op_node'
        match op_node:
            case tablescanNode():
                new_op_node = ScanNode(
                    op_node.table_name,
                    op_node.table_columns,
                    op_node.tableRestrictions,
                    op_node.tableFilters
                )
            case groupbyNode():
                new_op_node = GroupNode(
                    op_node.keyExpressions,
                    op_node.aggregateExpressions,
                    op_node.aggregateOperations
                )
            case executiontargetNode():
                new_op_node = OutputNode(
                    op_node.output_columns,
                    op_node.output_names
                )
            case joinNode():
                new_op_node = JoinNode(
                    op_node.joinMethod,
                    op_node.joinType,
                    op_node.joinCondition,
                    op_node.leftKeys,
                    op_node.rightKeys
                )
            case sortNode():
                sort_node = SortNode(
                    op_node.sortCriteria
                )
                # Create a limitNode if we have that
                if op_node.limitValue != None:
                    new_op_node = LimitNode(
                        op_node.limitValue
                    )
                    # Only add if not an empty list
                    if op_node.sortCriteria != []:
                        new_op_node.addChild(
                            sort_node
                        )
                else:
                    new_op_node = sort_node
            case mapNode():
                new_op_node = NewColumnNode(
                    op_node.mapValues
                )
            case groupjoinNode():
                # Split and convert into 2 nodes:
                #       Group after Join
                new_op_node = GroupNode(
                    op_node.groupKeys,
                    op_node.leftExpressions + op_node.rightExpressions,
                    op_node.leftAggregates + op_node.rightAggregates
                )
                new_op_node.addChild(
                    JoinNode(
                        None,
                        op_node.joinType,
                        equal_left_right_keys(op_node.leftKey, op_node.rightKey),
                        op_node.leftKey,
                        op_node.rightKey
                    )
                )
                assert hasattr(op_node, "cardinality")
                new_op_node.child.setCardinality(op_node.cardinality)
            case selectNode():
                new_op_node = FilterNode(
                    op_node.selectCondition
                )
            case explicitscanNode():
                if op_node.isRetrieve == False:
                    # Set as childNode, we can skip explicitScans
                    # As they just rename stuff
                    return childNode
                else:
                    assert hasattr(op_node, "targetOperator")
                    new_op_node = RetrieveNode(
                        op_node.table_columns,
                        op_node.targetOperator
                    )
                    
            case _:
                raise Exception(f"Unexpected op_node, it was of class: {op_node.__class__}")

        # Overwrite the existing OpNode
        lowest_node_pointer = new_op_node
        # Find the lowest node
        searching = True
        while searching == True:
            match lowest_node_pointer:
                case UnaryBaseNode():
                    if lowest_node_pointer.child != None:
                        lowest_node_pointer = lowest_node_pointer.child
                    else:
                        searching = False
                case _:
                    searching = False
        
        # Add in the children
        match lowest_node_pointer:
            case UnaryBaseNode():
                lowest_node_pointer.addChild(childNode)
            case BinaryBaseNode():
                lowest_node_pointer.addLeft(leftNode)
                lowest_node_pointer.addRight(rightNode)
            case _:
                # ScanNode
                assert isinstance(lowest_node_pointer, ScanNode)
                
        # Add hyperID to new_op_node
        assert hasattr(op_node, "hyperID")
        new_op_node.addID(op_node.hyperID)
        # Add cardinality to new_op_node
        assert hasattr(op_node, "cardinality")
        new_op_node.setCardinality(op_node.cardinality)
        
        return new_op_node

    return visit_hyper_nodes(op_tree)

from expression_operators import *
import datetime
import struct

def transform_hyper_iu_references(op_tree: HyperBaseNode):
    def hyper_restriction_parsing(restriction, table_columns, iu_references):
        current_restriction = None
        # Preorder Traversal
        if restriction["mode"] in ["<", "<=", ">", "=", "<>"]:
            if restriction["mode"] == "<":
                current_restriction = LessThanOperator()
            elif restriction["mode"] == ">":
                current_restriction = GreaterThanOperator()
            elif restriction["mode"] == "=":
                current_restriction = EqualsOperator()
            elif restriction["mode"] == "<>":
                current_restriction = NotEqualsOperator()
            elif restriction["mode"] == "<=":
                current_restriction = LessThanEqOperator()
            else:
                raise Exception(f"Unknown supposedly binary mode: {restriction['mode']}")
            
            current_restriction.addLeft(table_columns[restriction["attribute"]])
            if "value" not in restriction or "value2" in restriction:
                raise Exception(f"Unexpected keys in the restriction object: {restriction.keys()}")
            current_restriction.addRight(hyper_expression_parsing(restriction["value"], iu_references))
        elif restriction["mode"] in IntervalNotionOperator.SUPPORTED_MODES:
            # Using Interval Notion for the inequalities
            current_restriction = IntervalNotionOperator(restriction["mode"], table_columns[restriction["attribute"]])
            if "value" not in restriction or "value2" not in restriction or "value3" in restriction:
                raise Exception(f"Unexpected keys in the restriction object: {restriction.keys()}")
            current_restriction.addLeft(hyper_expression_parsing(restriction["value"], iu_references))
            current_restriction.addRight(hyper_expression_parsing(restriction["value2"], iu_references))
        elif restriction["mode"] == "lambda":
            assert restriction["value2"] is None
            current_restriction = hyper_expression_parsing(restriction["value"], iu_references)
        else:
            raise Exception(f"Unexpected mode for restriction: {restriction['mode']}")
        
        return current_restriction
    
    def parse_hyper_double(long: int) -> float:
        longint_binary = struct.pack('q', long)
        return struct.unpack('d', longint_binary)[0]
    
    def parse_hyper_date(incoming_date_number):
        confirmed_pairs = {
            2449354: datetime.date(1994, 1, 1),
            2449719: datetime.date(1995, 1, 1),
            2449792: datetime.date(1995, 3, 15)
        }
        
        # Start with top 1
        base_date_number, base_dt = list(confirmed_pairs.items())[0]
        date_number_difference = incoming_date_number - base_date_number
        created_dt_object = base_dt + datetime.timedelta(date_number_difference)
        
        # Verify against all others in dict
        assert created_dt_object - base_dt == datetime.timedelta(date_number_difference)
        for date_key, date_value in confirmed_pairs.items():
            date_value_difference = created_dt_object - date_value
            date_key_difference = incoming_date_number - date_key
            assert date_value_difference == datetime.timedelta(date_key_difference)
            
        return created_dt_object
    
    def parse_hyper_constant(expression) -> ConstantValue:
            const_value = expression["value"]
            const_type = None# Parse the type
            if expression["type"][0] == "Integer":
                const_value = int(const_value)
                const_type = "Integer"
            elif expression["type"][0] == "Date":
                const_value = parse_hyper_date(const_value)
                const_type = "Datetime"
            elif expression["type"][0] == "Double":
                const_value = parse_hyper_double(const_value)
                const_type = "Float"
            elif expression["type"][0] == "Varchar":
                const_type = "String"
            elif expression["type"][0] == "Numeric":
                const_value = const_value / int("1" + ("0"*expression["type"][2]))
                const_type = "Float"
            elif expression["type"][0] == "Bool":
                const_value = const_value
                const_type = "Bool"
            else:
                raise Exception(f"Unrecognised constant value type: {expression['type']}")
            return ConstantValue(const_value, const_type)
        
    def hyper_expression_parsing(expression, iu_references):
        current_op = None
        # Preorder Traversal
        # Is it a binary operator
        if expression["expression"] in ["mul", "div", "comparison", "sub", "add"]:
            if expression["expression"] == "mul":
                current_op = MulOperator()
            elif expression["expression"] == "div":
                current_op = DivOperator()
            elif expression["expression"] == "sub":
                current_op = SubOperator()
            elif expression["expression"] == "add":
                current_op = AddOperator()
            elif expression["expression"] == "comparison":
                if expression["mode"] in ["=", "is"]:
                    current_op = EqualsOperator()
                elif expression["mode"] == ">":
                    current_op = GreaterThanOperator()
                elif expression["mode"] == "<":
                    current_op = LessThanOperator()
                elif expression["mode"] == "<=":
                    current_op = LessThanEqOperator()
                elif expression["mode"] == "<>":
                    current_op = NotEqualsOperator()
                else:
                    raise Exception(f"Unknown operator that is allegedly a binary comparison one: {expression['expression']}")
            else:
                raise Exception(f"Unknown operator that is allegedly binary: {expression['expression']}")
            current_op.addLeft(hyper_expression_parsing(expression['left'], iu_references))
            current_op.addRight(hyper_expression_parsing(expression['right'], iu_references))
        # Is it a unary operator
        elif expression["expression"] in ["not", "extractyear"]:
            if expression["expression"] == "not":
                current_op = NotOperator()
            elif expression["expression"] == "extractyear":
                current_op = ExtractYearOperator()
            else:
                raise Exception(f"Unknown operator that is allegedly unary: {expression['expression']}")
            
            current_op.addChild(hyper_expression_parsing(expression["input"], iu_references))
        elif expression["expression"] == "quantor":
            if expression["mode"] == "=some":
                current_op = InSetOperator()
                current_op.addChild(hyper_expression_parsing(expression["value"], iu_references))
                for value in expression["set"]:
                    current_op.addToSet(hyper_expression_parsing(value, iu_references))
            else:
                raise Exception(f"Unrecognised mode for quantor expression: {expression['mode']}")
        elif expression["expression"] in ["and", "or"]:
            assert len(expression['arguments']) >= 2
            joinExpression = None
            if expression["expression"] == "and":
                joinExpression = "AndOperator"
            elif expression["expression"] == "or":
                joinExpression = "OrOperator"
            else:
                raise Exception(f"Unrecognised expression with many arguments: {expression['expression']}")
            remainingNodeArguments = [hyper_expression_parsing(exp, iu_references) for exp in expression["arguments"]]
            current_op = join_statements_with_operator(remainingNodeArguments, joinExpression)
        elif expression["expression"] == "iuref":
            if expression["iu"] not in iu_references:
                raise Exception(f"Trying to get a value ({expression['iu']}) from iu_references, that doesn't exist")
            current_op = iu_references[expression["iu"]]
        elif expression["expression"] == "const":
            current_op = parse_hyper_constant(expression["value"])
        elif expression["expression"] == "simplecase":
            current_op = CaseOperator()
            specifiedValue = hyper_expression_parsing(expression["value"], iu_references)
            for caseInstance in expression["cases"]:
                equalledCases = []
                for case in caseInstance["cases"]:
                    localEqOperator = EqualsOperator()
                    localEqOperator.addLeft(specifiedValue)
                    localEqOperator.addRight(hyper_expression_parsing(case, iu_references))
                    equalledCases.append(localEqOperator)
                
                if len(equalledCases) == 1:
                    constructCases = equalledCases[0]
                elif len(equalledCases) >= 2:
                    constructCases = join_statements_with_operator(equalledCases, "OrOperator")
                else:
                    raise Exception(f"Unacceptable")
                
                caseInstanceObject = CaseInstance()
                caseInstanceObject.setCase(constructCases)
                caseInstanceObject.setOutputValue(hyper_expression_parsing(caseInstance["value"], iu_references))
                
                current_op.addToCase(caseInstanceObject)
            
            current_op.addElse(hyper_expression_parsing(expression["else"], iu_references))
        elif expression["expression"] == "case":
            current_op = CaseOperator()
            for caseInstance in expression["cases"]:
                # Make a case instance
                currentCaseInstance = CaseInstance()
                currentCaseInstance.setOutputValue(hyper_expression_parsing(caseInstance['value'], iu_references))
                currentCaseInstance.setCase(hyper_expression_parsing(caseInstance['case'], iu_references))
                
                current_op.addToCase(currentCaseInstance)
            current_op.addElse(hyper_expression_parsing(expression["else"], iu_references))
        elif expression["expression"] == "like":
            assert len(expression["arguments"]) == 3
            likeExtraValue = hyper_expression_parsing(expression["arguments"][2], iu_references)
            targetLikeExtraValue = ConstantValue("\\", "String")
            assert likeExtraValue == targetLikeExtraValue
            current_op = LikeOperator(
                # Value
                hyper_expression_parsing(expression["arguments"][0], iu_references),
                # Comparator
                hyper_expression_parsing(expression["arguments"][1], iu_references)
            )
        elif expression["expression"] == "between":
            assert len(expression["arguments"]) == 3
            value = hyper_expression_parsing(expression["arguments"][0], iu_references)
            current_op = IntervalNotionOperator("[]", value)
            current_op.addLeft(hyper_expression_parsing(expression["arguments"][1], iu_references))
            current_op.addRight(hyper_expression_parsing(expression["arguments"][2], iu_references))
        elif expression["expression"] == "substring":
            assert len(expression["arguments"]) == 3
            value = hyper_expression_parsing(expression["arguments"][0], iu_references)
            startPos = hyper_expression_parsing(expression["arguments"][1], iu_references)
            # startPos is 1-indexed, we will use 0 throughout
            startPos.value = startPos.value - 1
            length = hyper_expression_parsing(expression["arguments"][2], iu_references)
            current_op = SubstringOperator(value, startPos, length)
        elif expression["expression"] == "lookup":
            inputValues = []
            for val in expression["input"]:
                inputValues.append(hyper_expression_parsing(val, iu_references))
            inputComparisons = []
            for comp in expression["values"]:
                inputComparisons.append(parse_hyper_constant(comp))
            inputModes = []
            for mode in expression["modes"]:
                if mode == "is":
                    inputModes.append(EqualsOperator())
                else:
                    raise Exception(f"Unknown comparison mode for lookup operator: {mode}")
            current_op = LookupOperator(inputValues, inputComparisons, inputModes)
        else:
            raise Exception(f"Unexpected Expression type discovered: {expression['expression']}")
        
        assert current_op != None
        return current_op
    
    def replace_key_expressions_using_iu_references(key_expressions_list: list, iu_references: dict) -> list:
        newKeyExpressions = []
        for keyExpression in key_expressions_list:
            if keyExpression["expression"]["value"]["expression"] != "iuref":
                raise Exception("Key Expressions list refer to things other than iurefs")
            current_iu_name = keyExpression["expression"]["value"]["iu"]
            # Remove the object at this key
            iu_object = iu_references.pop(current_iu_name)
            new_iu_name = keyExpression["iu"][0]
            # Insert it back in with the new key
            iu_references[new_iu_name] = iu_object
            # And add it to the this
            newKeyExpressions.append(iu_object)
        return newKeyExpressions
    
    def replace_expressions_using_iu_references(expressions_list: list, iu_references: dict) -> list:
        newExpressionList = []
        for expression in expressions_list:
            if "value" in expression:
                newExpressionList.append(hyper_expression_parsing(expression["value"], iu_references))
            elif "expression" in expression:
                newExpressionList.append(hyper_expression_parsing(expression, iu_references))
            else:
                raise Exception("Unknown expression list format")
            
        return newExpressionList
    
    def replace_aggregations_using_iu_and_expressions(aggregations_list: list, iu_references: dict, expressions_list: list) -> list:
        newAggregateOperations = []
        for aggr_op in aggregations_list:
            newAggrOp = None
            if aggr_op['operation']['aggregate'] in ["avg", "sum", "count", "min", "max"]:
                if aggr_op['operation']['aggregate'] == "sum":
                    newAggrOp = SumAggrOperator()
                elif aggr_op['operation']['aggregate'] == "min":
                    newAggrOp = MinAggrOperator()
                elif aggr_op['operation']['aggregate'] == "avg":
                    newAggrOp = AvgAggrOperator()
                elif aggr_op['operation']['aggregate'] == "count":
                    if "distinct" in aggr_op['operation']:
                        assert aggr_op["operation"]["distinct"] == True
                        newAggrOp = CountDistinctAggrOperator()
                    else:
                        newAggrOp = CountAggrOperator()
                elif aggr_op['operation']['aggregate'] == "max":
                    newAggrOp = MaxAggrOperator()
                else:
                    raise Exception(f"Unrecognised Aggregation operator: {aggr_op['operation']['aggregate']}")
                    
                if aggr_op["source"] >= len(expressions_list):
                    if aggr_op['operation']['aggregate'] == "count":
                        newAggrOp = CountAllOperator()
                    else:
                        raise Exception(f"The source ({aggr_op['source']}) was too long for the expressions of length: {len(expressions_list)}")
                else:
                    newAggrOp.addChild(expressions_list[aggr_op["source"]])
                # Save as a new iu
                assert "v" in aggr_op["iu"][0]
                iu_references[aggr_op["iu"][0]] = newAggrOp
                # Store in list
                newAggregateOperations.append(newAggrOp)
            else:
                raise Exception(f"Unknown aggregation operator, it was {aggr_op['operation']}")        
        return newAggregateOperations
    
    def determine_join_left_right_keys(op_node: HyperBaseNode, left_ius: dict, right_ius: dict) -> None:
        # Set the left and right keys for the JoinNode
        determined_left_keys = []
        determined_right_keys = []
        
        # Get all join keys - these are things not in the following list
        notJoinKeyTypes = set([type(x) for x in [AndOperator(), OrOperator(), EqualsOperator(), NotEqualsOperator(), GreaterThanEqOperator(),
                           GreaterThanOperator(), LessThanEqOperator(), LessThanOperator(), ConstantValue("", "String")]])
        def traverse_to_surface_keys(joinCondition: ExpressionBaseNode, initial_keys = []) -> list:
            gathering_surfaces = []
            if type(joinCondition) not in notJoinKeyTypes:
                pass
            elif isinstance(joinCondition, BinaryExpressionOperator): 
                left_surfaces = traverse_to_surface_keys(joinCondition.left, gathering_surfaces)
                right_surfaces = traverse_to_surface_keys(joinCondition.right, gathering_surfaces)
                gathering_surfaces.extend(left_surfaces)
                gathering_surfaces.extend(right_surfaces)
            elif isinstance(joinCondition, UnaryExpressionOperator):
                child_surfaces = traverse_to_surface_keys(joinCondition.child, gathering_surfaces)
                gathering_surfaces.extend(child_surfaces)
            else:
                pass    
            
            if type(joinCondition) not in notJoinKeyTypes:
                if isinstance(joinCondition, InSetOperator):
                    gathering_surfaces.append(joinCondition.child)
                elif isinstance(joinCondition, IntervalNotionOperator):
                    gathering_surfaces.append(joinCondition.value)
                elif isinstance(joinCondition, LookupOperator):
                    gathering_surfaces.extend(joinCondition.values)
                else:
                    gathering_surfaces.append(joinCondition)
                
            return gathering_surfaces
            
        all_join_keys = traverse_to_surface_keys(op_node.joinCondition)
        
        # Double check correct - there should be none of the notJoinKey types in the output
        assert (set([type(x) for x in all_join_keys]).intersection(notJoinKeyTypes)) == set()
         
        # Filter using IU information for left and right
        leftIUColumnIDs = [id(x) for x in left_ius.values()]
        rightIUColumnIDs = [id(x) for x in right_ius.values()]
        
        for join_key in all_join_keys:
            if id(join_key) in leftIUColumnIDs:
                determined_left_keys.append(join_key)
            elif id(join_key) in rightIUColumnIDs:
                determined_right_keys.append(join_key)
            else:
                raise Exception("Join Key not found in either left or right")
    
        assert len(determined_left_keys) > 0 and len(determined_right_keys) > 0
    
        # Avoid duplicates
        op_node.setLeftKeys(list(set(determined_left_keys)))
        op_node.setRightKeys(list(set(determined_right_keys)))
    
    def visit_solve_iu_references(op_node: HyperBaseNode , iu_references: dict):
        # Visit Children
        if isinstance(op_node, JoinBaseNode) and op_node.isJoinNode == True:
            left_dict = visit_solve_iu_references(op_node.left, {})
            right_dict = visit_solve_iu_references(op_node.right, {})
            iu_references.update(left_dict)
            iu_references.update(right_dict)
        elif op_node.child != None:
            child_dict = visit_solve_iu_references(op_node.child, {})
            iu_references.update(child_dict)
        else:
            # A leaf node
            pass
        
        # Children visited, now process current node
        if isinstance(op_node, tablescanNode):
            newTableColumns = []
            for column in op_node.table_columns:
                currentColumn = None
                if column['iu'] != None:
                    currentColumn = ColumnValue(
                        column['name'],
                        column["type"]
                    )
                    iu_references[column['iu'][0]] = currentColumn
                    # Has an iu reference, so it's essential
                    currentColumn.setEssential(True)
                else:
                    currentColumn = ColumnValue(
                        column['name'],
                        column["type"]
                    )
                newTableColumns.append(currentColumn)
            op_node.table_columns = newTableColumns    
            newTableRestrictions = []
            for restriction in op_node.tableRestrictions:
                newTableRestrictions.append(hyper_restriction_parsing(restriction, op_node.table_columns, iu_references))
            # Join all the restrictions together
            if len(newTableRestrictions) >= 2:
                op_node.tableRestrictions = join_statements_with_operator(newTableRestrictions, "AndOperator")
            elif len(newTableRestrictions) == 1:
                op_node.tableRestrictions = newTableRestrictions[0]
            else:
                op_node.tableRestrictions = newTableRestrictions
            newTableFilters = []
            for aFilter in op_node.tableFilters:
                newTableFilters.append(hyper_expression_parsing(aFilter, iu_references))
            # Join all the restrictions together
            # Filter bare Booleans
            def bareBoolean(x):
                if isinstance(x, ConstantValue):
                    if (x.type == 'Bool' and x.value == True):
                        return False
                    else:
                        return True
                else:
                    return True
                
            newTableFilters = list(filter(bareBoolean, newTableFilters))
            
            if len(newTableFilters) >= 2:
                op_node.tableFilters = join_statements_with_operator(newTableFilters, "AndOperator")
            elif len(newTableFilters) == 1:
                op_node.tableFilters = newTableFilters[0]
            else:
                op_node.tableFilters = newTableFilters
        elif isinstance(op_node, groupbyNode):
            newExpressionList = replace_expressions_using_iu_references(op_node.aggregateExpressions, iu_references)
            op_node.aggregateExpressions = newExpressionList
            newAggregateOperations = replace_aggregations_using_iu_and_expressions(op_node.aggregateOperations, iu_references, newExpressionList)
            op_node.aggregateOperations = newAggregateOperations
            newKeyExpressionsList = replace_key_expressions_using_iu_references(op_node.keyExpressions, iu_references)
            op_node.keyExpressions = newKeyExpressionsList
        elif isinstance(op_node, executiontargetNode):
            for idx, outputColumn in enumerate(op_node.output_columns):
                if outputColumn["expression"] == "iuref":
                    op_node.output_columns[idx] = iu_references[outputColumn["iu"][0]]
                else:
                    raise Exception(f"Unexpected format for output columns")
        elif isinstance(op_node, joinNode):
            newJoinConditionList = hyper_expression_parsing(op_node.joinCondition, iu_references)
            op_node.joinCondition = newJoinConditionList
            # Determine if the keys come from the left or right
            determine_join_left_right_keys(op_node, left_dict, right_dict)
        elif isinstance(op_node, sortNode):
            newSortCriteria = []
            for sortCriterion in op_node.sortCriteria:
                if sortCriterion["value"]["expression"] == "iuref":
                    newSortCriteria.append(SortOperator(iu_references[sortCriterion["value"]["iu"]], sortCriterion["descending"]))
                else:
                    raise Exception(f"Unexpected format for output columns")
            op_node.sortCriteria = newSortCriteria
        elif isinstance(op_node, mapNode):
            newMapValues = []
            for mapValue in op_node.mapValues:
                # Set iu into iu_references
                assert mapValue['iu'][0] not in iu_references
                parsedValue = hyper_expression_parsing(mapValue['value'], iu_references)
                iu_references[mapValue['iu'][0]] = parsedValue
                newMapValues.append(parsedValue)
            op_node.mapValues = newMapValues
        elif isinstance(op_node, groupjoinNode):
            # leftKey, rightKey
            newLeftKeys = []
            for leftEntry in op_node.leftKey:
                if "expression" and "iu" in leftEntry:
                    # Have to do replace in iu_references
                    assert leftEntry['iu'][0] not in iu_references
                    parsedValue = hyper_expression_parsing(leftEntry['expression']['value'], iu_references)
                    iu_references[leftEntry['iu'][0]] = parsedValue
                    op_node.groupKeys.append(parsedValue)
                else:
                    assert leftEntry['value']['iu'] in iu_references
                    parsedValue = iu_references[leftEntry['value']['iu']]
                newLeftKeys.append(parsedValue)
            op_node.leftKey = newLeftKeys
            newRightKeys = []
            for rightEntry in op_node.rightKey:
                if "expression" and "iu" in rightEntry:
                    # Have to do replace in iu_references
                    assert rightEntry['iu'][0] not in iu_references
                    parsedValue = hyper_expression_parsing(rightEntry['value'], iu_references)
                    iu_references[rightEntry['iu'][0]] = parsedValue
                    op_node.groupKeys.append(parsedValue)
                else:
                    assert rightEntry['value']['iu'] in iu_references
                    parsedValue = iu_references[rightEntry['value']['iu']]
                newRightKeys.append(parsedValue)
            op_node.rightKey = newRightKeys
            # Expressions
            newLeftExpressionList = replace_expressions_using_iu_references(op_node.leftExpressions, iu_references)
            op_node.leftExpressions = newLeftExpressionList
            newRightExpressionList = replace_expressions_using_iu_references(op_node.rightExpressions, iu_references)
            op_node.rightExpressions = newRightExpressionList
            # Aggregates
            newOverallExpressionsList = op_node.groupKeys + newLeftExpressionList + newRightExpressionList
            newLeftAggregateOperations = replace_aggregations_using_iu_and_expressions(op_node.leftAggregates, iu_references, newOverallExpressionsList)
            op_node.leftAggregates = newLeftAggregateOperations
            newRightAggregateOperations = replace_aggregations_using_iu_and_expressions(op_node.rightAggregates, iu_references, newOverallExpressionsList)
            op_node.rightAggregates = newRightAggregateOperations
        elif isinstance(op_node, selectNode):
            newSelectCondition = hyper_expression_parsing(op_node.selectCondition, iu_references)
            op_node.selectCondition = newSelectCondition
        elif isinstance(op_node, explicitscanNode):
            # set TableColumns
            newTableColumns = []
            for column in op_node.mapping:
                assert "target" in column and "source" in column
                columnTargetIU = column["target"][0]
                assert columnTargetIU not in iu_references
                if isinstance(column["source"]["iu"], list):
                    columnSourceIU = column["source"]["iu"][0]
                else:
                    columnSourceIU = column["source"]["iu"]
                assert columnSourceIU in iu_references
                # Add to table columns
                newTableColumns.append(iu_references[columnSourceIU])
                iu_references[columnTargetIU] = iu_references[columnSourceIU]
            op_node.table_columns = newTableColumns
        else:
            raise Exception(f"Unsure how to deal with {op_node.__class__} node.")
        
        return iu_references
    
    iu_references = dict()
    visit_solve_iu_references(op_tree, iu_references)

# # generate_hyperdb_explains()
# # inspect_explain_plans()
# # parse_explain_plans()
# convert_explain_plan_to_x("sdqlpy")
