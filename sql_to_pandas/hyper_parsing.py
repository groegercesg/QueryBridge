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

def generate_hyperdb_explains():
    db = PrepareHyperDB('hyperdb_tpch.hyper')
    db.prepare_database('data_storage')
    query_directory = 'sql_to_pandas/tpch_queries'
    explain_directory = 'sql_to_pandas/hyperdb_tpch_explain'
    
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
    
    if explain_node["operator"] == "select":
        print("a")
    
    operators.add(explain_node["operator"])
    return operators

def inspect_explain_plans():
    explain_directory = 'sql_to_pandas/hyperdb_tpch_explain'
    
    onlyfiles = [f for f in listdir(explain_directory) if isfile(join(explain_directory, f))]
    
    operators = set()
    
    for explain_file in onlyfiles:
        with open(f'{explain_directory}/{explain_file}') as r:
            explain_content = json.loads(r.read())

        operators.update(gather_operators(explain_content))
    
    print("Below are the operators that Hyper DB Uses:")
    print(operators)

from hyper_classes import *

def str_to_class(classname):
    return getattr(sys.modules[__name__], classname)

def create_operator_tree(explain_json):
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
        if "restrictions" in explain_json:
            tableRestrictions = explain_json["restrictions"]
        operator_class = tablescanNode(explain_json["debugName"]["value"], explain_json["values"], tableRestrictions)
    elif operator_name == "sort":
        operator_class = sortNode(explain_json["criterion"])
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
    else:
        raise Exception(f"No Operator Class Creation has been defined for the {operator_name} operator.")

    # Validate children operator options are as expected
    if ("input" in explain_json) and (any(x in explain_json for x in ["left", "right"])):
        raise Exception("There shouldn't be 'input' as well as either 'left'/'right' parameters for an operator")
    
    # Add children to the different operators
    if ("input" in explain_json):
        operator_class.addChild(create_operator_tree(explain_json["input"]))
    elif any(x in explain_json for x in ["left", "right"]):
        # It should be an operator type if we're here
        assert isinstance(operator_class, JoinNode) and hasattr(operator_class, "isJoinNode") and operator_class.isJoinNode == True
        operator_class.addLeft(create_operator_tree(explain_json["left"]))
        operator_class.addRight(create_operator_tree(explain_json["right"]))
    else:
        # Only table scan node should have no children
        assert isinstance(operator_class, tablescanNode)
    
    return operator_class
    
def parse_explain_plans():
    explain_directory = 'sql_to_pandas/hyperdb_tpch_explain'
    
    onlyfiles = [f for f in listdir(explain_directory) if isfile(join(explain_directory, f))]
    
    all_operator_trees = []
    for explain_file in onlyfiles:
        if explain_file.split("_")[0] not in ["3", "6"]: #  
            continue
         
        print(f"Transforming {explain_file} into a Hyper Tree")
        with open(f'{explain_directory}/{explain_file}') as r:
            explain_content = json.loads(r.read())
        
        op_tree = create_operator_tree(explain_content)
        all_operator_trees.append(op_tree)

    #assert len(all_operator_trees) == 21
    print("All 21 HyperDB plans have been parsed into Hyper DB Class Trees")
    
    # Transform the operator_trees
    # Task 1: Solve the 'v1' references, in 'iu'
        # These propagate up, and are occasionally reset by things like "mapNode" or "groupbyNode"
        # Postorder traversal is required for this, make a dict of the pairs:
            # v1: l_suppkey
            # v3: p_partkey
            # ...
        # Also in this pass we should parse expressions (and similar things) into ExpressionOperators
    for tree in all_operator_trees:
        transform_hyper_iu_references(tree)
    # Task 2: ...
    
    print("Hyper Tree Plans have been Transformed")

from expression_operators import *
import datetime

def transform_hyper_iu_references(op_tree: HyperBaseNode):
    def hyper_restriction_parsing(restriction, table_columns, iu_references):
        current_restriction = None
        # Preorder Traversal
        if restriction["mode"] in ["<", ">", "="]:
            if restriction["mode"] == "<":
                current_restriction = LessThanOperator()
            elif restriction["mode"] == ">":
                current_restriction = GreaterThanOperator()
            elif restriction["mode"] == "=":
                current_restriction = EqualsOperator()
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
        else:
            raise Exception(f"Unexpected mode for restriction: {restriction['mode']}")
        
        return current_restriction
    
    def parse_hyper_double(incoming_double_number):
        print(f"We need to complete the parse_hyper_double method")
        return None
    
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
        
    def hyper_expression_parsing(expression, iu_references):
        current_op = None
        # Preorder Traversal
        # Is it a binary operator
        if expression["expression"] in ["mul", "comparison", "sub"]:
            if expression["expression"] == "mul":
                current_op = MulOperator()
            elif expression["expression"] == "sub":
                current_op = SubOperator()
            elif expression["expression"] == "comparison":
                if expression["mode"] == "=":
                    current_op = EqualsOperator()
                else:
                    raise Exception(f"Unknown operator that is allegedly a binary comparison one: {expression['expression']}")
            else:
                raise Exception(f"Unknown operator that is allegedly binary: {expression['expression']}")
            current_op.addLeft(hyper_expression_parsing(expression['left'], iu_references))
            current_op.addRight(hyper_expression_parsing(expression['right'], iu_references))
        elif expression["expression"] == "iuref":
            if expression["iu"] not in iu_references:
                raise Exception(f"Trying to get a value ({expression['iu']}) from iu_references, that doesn't exist")
            current_op = ColumnValue(iu_references[expression["iu"]])
        elif expression["expression"] == "const":
            const_value = expression["value"]["value"]
            const_type = None# Parse the type
            if expression["value"]["type"][0] == "Integer":
                const_value = int(const_value)
                const_type = "Integer"
            elif expression["value"]["type"][0] == "Date":
                const_value = parse_hyper_date(const_value)
                const_type = "Datetime"
            elif expression["value"]["type"][0] == "Double":
                const_value = parse_hyper_double(const_value)
                const_type = "Float"
            elif expression["value"]["type"][0] == "Varchar":
                const_type = "String"
            else:
                raise Exception(f"Unrecognised constant value type: {expression['value']['type']}")
            current_op = ConstantValue(const_value, const_type)
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
            else:
                raise Exception("Unknown expression list format")
            
        return newExpressionList
    
    def replace_aggregations_using_iu_and_expressions(aggregations_list: list, iu_references: dict, expressions_list: list) -> list:
        newAggregateOperations = []
        for aggr_op in aggregations_list:
            newAggrOp = None
            if aggr_op['operation']['aggregate'] == "sum":
                newAggrOp = SumAggrOperator()
                if aggr_op["source"] >= len(expressions_list):
                    raise Exception(f"The source ({aggr_op['source']}) was too long for the expressions of length: {len(expressions_list)}")
                newAggrOp.addChild(expressions_list[aggr_op["source"]])
                # Save as a new iu
                iu_references[aggr_op["iu"][0]] = newAggrOp
                # Store in list
                newAggregateOperations.append(newAggrOp)
            else:
                raise Exception(f"Unknown aggregation operator, it was {aggr_op['operation']}")        
        return newAggregateOperations
    
    def visit_solve_iu_references(op_node: HyperBaseNode , iu_references: dict):
        # Visit Children
        if isinstance(op_node, JoinNode) and op_node.isJoinNode == True:
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
                    currentColumn = ColumnValue(column['name'])
                    iu_references[column['iu'][0]] = currentColumn
                else:
                    currentColumn = ColumnValue(column['name'])
                newTableColumns.append(currentColumn)
            op_node.table_columns = newTableColumns    
            newTableRestrictions = []
            for restriction in op_node.tableRestrictions:
                newTableRestrictions.append(hyper_restriction_parsing(restriction, op_node.table_columns, iu_references))
            op_node.tableRestrictions = newTableRestrictions
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
        elif isinstance(op_node, sortNode):
            newSortCriteria = []
            for sortCriterion in op_node.sortCriteria:
                if sortCriterion["value"]["expression"] == "iuref":
                    newSortCriteria.append(SortOperator(iu_references[sortCriterion["value"]["iu"]], sortCriterion["descending"]))
                else:
                    raise Exception(f"Unexpected format for output columns")
            op_node.sortCriteria = newSortCriteria
        else:
            raise Exception(f"Unsure how to deal with {op_node.__class__} node.")
        
        return iu_references
        
        
    iu_references = dict()
    visit_solve_iu_references(op_tree, iu_references)
    
    

#generate_hyperdb_explains()
#inspect_explain_plans()
parse_explain_plans()
