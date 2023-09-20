from collections import defaultdict
from copy import deepcopy
import functools
import sys

class TreeHelper():
    def __init__(self, expr_tree_output_path, benchmarking, in_use_numpy, in_groupbyfusion, in_mergefusion):
        # Dictionary to track how many "filter" nodes there have been
        self.node_type_tracker = defaultdict(int)
        # Dictionary to map class: filter_node (id: 123123) to df_name: df_filter_2
        self.node_id_tracker = {}
        # Counter to count number of expression trees output
        self.expr_tree_tracker = 1
        self.expr_output_path = expr_tree_output_path
        # Store whether we are benchmarking
        self.bench = benchmarking
        self.use_numpy = in_use_numpy
        self.groupby_fusion = in_groupbyfusion
        self.merge_fusion = in_mergefusion

class CodeCompilation():
    def __init__(self, sql_class, column_ordering, column_limiting):
        self.usePostAggr = False
        self.indexes = []
        self.relations = []
        self.sql = sql_class
        self.column_ordering = column_ordering
        self.column_limiting = column_limiting
        self.bracket_replace = {}
        self.useAlias = {}
        self.aliasRelationPairs = {}
        
    def setAggr(self, aggr):
        self.usePostAggr = aggr
        
    def setIndexes(self, idxs):
        if isinstance(idxs, list):
            self.indexes = idxs
        else:
            self.indexes = [idxs]
        
    def add_relation(self, relation):
        self.relations.append(relation)
        
    def add_bracket_replace(self, old_name, new_name):
        if self.bracket_replace.get(old_name, None) == None:
            # Not in dictionary, add it
            self.bracket_replace[old_name] = new_name
        else:
            # Exists in dictionary, ignore it
            pass
        
    def __add__(self, other):
        # bracket_replace
        if (int(sys.version_info[1]) < 9):
            new_bracket_replace = {**self.bracket_replace, **other.bracket_replace}
        else:
            new_bracket_replace = self.bracket_replace | other.bracket_replace
        # column_ordering
        new_column_ordering = self.column_ordering or other.column_ordering
        # column_limiting
        new_column_limiting = self.column_limiting or other.column_limiting
        # indexes
        new_indexes = list(set(self.indexes + other.indexes))
        # relations
        new_relations = list(set(self.relations + other.relations))
        # sql
        new_sql = self.sql
        # usePostAggr
        new_use_post_aggr = self.usePostAggr or other.usePostAggr
        # useAlias
        if (int(sys.version_info[1]) < 9):
            new_use_alias = {**self.useAlias, **other.useAlias}
        else:
            new_use_alias = self.useAlias | other.useAlias
        # relationAliasPairs
        if (int(sys.version_info[1]) < 9):
            new_alias_relation_pairs = {**self.aliasRelationPairs, **other.aliasRelationPairs}
        else:
            new_alias_relation_pairs = self.aliasRelationPairs | other.aliasRelationPairs
        
        # Make class
        returningCodeComp = CodeCompilation(new_sql, new_column_ordering, new_column_limiting)
        returningCodeComp.usePostAggr = new_use_post_aggr
        returningCodeComp.relations = new_relations
        returningCodeComp.indexes = new_indexes
        returningCodeComp.bracket_replace = new_bracket_replace
        returningCodeComp.useAlias = new_use_alias
        returningCodeComp.aliasRelationPairs = new_alias_relation_pairs
        
        return returningCodeComp

def get_class_name(node):
    return str(str(node.__class__.__name__).split("_")[0])

def get_class_id(node):
    return str(id(node))

def make_pandas(pandas_tree, sql, args, treeHelper, output_name=None):
    # Function to generate pandas code from tree of classes
    pandas_statements = []
    # Process incoming SQL file using module
    from pandas_tree import sql_class
    sql_file = sql_class(sql)
    # Flag for using post-aggr output or not
    
    baseCodeCompHelper = CodeCompilation(sql_file, args.column_ordering, args.column_limiting)
    aggrs = ["aggr", "group"]
    
    ccHelper = postorder_traversal(pandas_tree, pandas_statements, baseCodeCompHelper, aggrs, treeHelper)
    
    if output_name == "RETURN":
        top_class_id = get_class_id(pandas_tree)
        if treeHelper.node_id_tracker.get(top_class_id, None) == None:
            raise ValueError("Top Node in tree not detected")
        else:
            last_df = treeHelper.node_id_tracker[top_class_id]
            pandas_statements.append("return " + str(last_df))
        
    
    elif output_name != None:
        top_class_id = get_class_id(pandas_tree)
        if treeHelper.node_id_tracker.get(top_class_id, None) == None:
            raise ValueError("Top Node in tree not detected")
        else:
            # current_df = pandas_statements[-1].split(" = ")[0]
            last_df = treeHelper.node_id_tracker[top_class_id]
        
        if isinstance(pandas_tree.output, list):
            # Use 1st element
            # Hardcoded at the moment to only handle one
            if isinstance(pandas_tree.output[0], tuple):
                last_column = str(pandas_tree.output[0][1])
            else:
                last_column = str(pandas_tree.output[0])
                
            # Check if last_column in in codeCompHelpers
            if ccHelper.bracket_replace.get(last_column, None) != None:
                last_column = ccHelper.bracket_replace.get(last_column, None)
        else:
            raise ValueError("We were expecting the output attribute of pandas_tree to be a list, but it's not! Output: " + str(pandas_tree.output))
        
        pandas_statements.append(str(output_name) + " = " + str(last_df) + "['" + last_column + "'][0]")
        pandas_statements.append("")
    
    return pandas_statements, ccHelper

def postorder_traversal(tree, pandas_statements, baseCodeCompHelper, aggrs, treeHelper):
    #if root is None return
    if tree==None:
        return None
    if hasattr(tree, 'nodes') and (tree.nodes != []):
        list_of_cc_helpers = []
        for node in tree.nodes:
            #traverse left subtree
            ccHelper = postorder_traversal(node, pandas_statements, baseCodeCompHelper, aggrs, treeHelper)
            list_of_cc_helpers.append(ccHelper)
        
        # Later I merge these
        # And we set the merged version to be: codeCompHelper
        if len(list_of_cc_helpers) == 1:
            codeCompHelper = list_of_cc_helpers[0]
        else:
            # We have many ccHelpers, we have to merge them
            codeCompHelper = functools.reduce(lambda x, y:x+y, list_of_cc_helpers)        
        
    else:
        # We are at a child
        # Make a copy of CCHelper
        # Copy baseCCHelper in ccHelper
        codeCompHelper = deepcopy(baseCodeCompHelper)
                
    # Process the node we're at, create the df_name for it
    current_class_name = get_class_name(tree)
    df_name = "df_" + current_class_name + "_" + str(treeHelper.node_type_tracker.get(current_class_name, 0) + 1)
    # Increment current class in node_type_tracker
    treeHelper.node_type_tracker[current_class_name] += 1
    # Store id and df_name in node_id_tracker
    current_class_id = get_class_id(tree)
    if treeHelper.node_id_tracker.get(current_class_id, None) == None:
        treeHelper.node_id_tracker[current_class_id] = df_name
    else:
        # We have an id collision, these are not unique
        raise ValueError("We have a node_id collision!")
        
    if hasattr(tree, 'nodes') and tree.nodes != []:
        # Not first time, figure out what previous df would be called
        prev_node_names = []
        for prev_node in tree.nodes:
            prev_node_id = get_class_id(prev_node)
            prev_node_names.append(treeHelper.node_id_tracker[prev_node_id])
        # Process prev_class_names
        if len(prev_node_names) == 1:
            prev_node_name = prev_node_names[0]
        else:
            # We have to handle multiple node below, this must be a MERGE!
            # We pass in a list of previous names, 
            prev_node_name = prev_node_names
            
        # Decide on what output to use
        if current_class_name in aggrs:
            # We are in the aggr
            codeCompHelper.setAggr(True)
            if hasattr(tree, "data"):
                codeCompHelper.add_relation(tree.data)
            
            if hasattr(tree, "alias") and hasattr(tree, "data"):
                # Assume that we NEVER have an alias but no data
                if tree.data != tree.alias:
                    # Add the Alias to relations
                    codeCompHelper.add_relation(tree.alias)
                    # Add the relation alias pair to ccHelper
                    codeCompHelper.aliasRelationPairs[tree.alias] = tree.data
            
            pandas_strings = tree.to_pandas(prev_node_name, df_name, codeCompHelper, treeHelper)
        else:
            if hasattr(tree, "data"):
                codeCompHelper.add_relation(tree.data)
                
            if hasattr(tree, "alias") and hasattr(tree, "data"):
                # Assume that we NEVER have an alias but no data
                if tree.data != tree.alias:
                    # Add the Alias to relations
                    codeCompHelper.add_relation(tree.alias)
                    # Add the relation alias pair to ccHelper
                    codeCompHelper.aliasRelationPairs[tree.alias] = tree.data
            
            pandas_strings = tree.to_pandas(prev_node_name, df_name, codeCompHelper, treeHelper)
        
    else:
        # Add tree.data, the relation name, to codeCompHelper
        if hasattr(tree, "data"):
            codeCompHelper.add_relation(tree.data)
            
        if hasattr(tree, "alias") and hasattr(tree, "data"):
            # Assume that we NEVER have an alias but no data
            if tree.data != tree.alias:
                # Add the Alias to relations
                codeCompHelper.add_relation(tree.alias)
                # Add the relation alias pair to ccHelper
                codeCompHelper.aliasRelationPairs[tree.alias] = tree.data
        
        pandas_strings = tree.to_pandas(tree.data, df_name, codeCompHelper, treeHelper)

   
    for statement in pandas_strings:
        pandas_statements.append(statement)
        
    return codeCompHelper
