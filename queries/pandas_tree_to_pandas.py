from collections import defaultdict
from copy import deepcopy
import functools


class TreeHelper():
    def __init__(self):
        # Dictionary to track how many "filter" nodes there have been
        self.node_type_tracker = defaultdict(int)
        # Dictionary to map class: filter_node (id: 123123) to df_name: df_filter_2
        self.node_id_tracker = {}

class CodeCompilation():
    def __init__(self, sql_class, column_ordering):
        self.usePostAggr = False
        self.indexes = []
        self.relations = []
        self.sql = sql_class
        self.column_ordering = column_ordering
        self.bracket_replace = {}
        
    def setAggr(self, aggr):
        self.usePostAggr = aggr
        
    def setIndexes(self, idxs):
        self.indexes = idxs
        
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
        new_bracket_replace = self.bracket_replace | other.bracket_replace
        # column_ordering
        new_column_ordering = self.column_ordering or other.column_ordering
        # indexes
        new_indexes = list(set(self.indexes + other.indexes))
        # relations
        new_relations = list(set(self.relations + other.relations))
        # sql
        new_sql = self.sql
        # usePostAggr
        new_use_post_aggr = self.usePostAggr or other.usePostAggr
        
        # Make class
        returningCodeComp = CodeCompilation(new_sql, new_column_ordering)
        returningCodeComp.usePostAggr = new_use_post_aggr
        returningCodeComp.relations = new_relations
        returningCodeComp.indexes = new_indexes
        returningCodeComp.bracket_replace = new_bracket_replace
        
        return returningCodeComp

def get_class_name(node):
    return str(str(node.__class__.__name__).split("_")[0])

def get_class_id(node):
    return str(id(node))

def make_pandas(pandas_tree, sql, precise_column_ordering):
    # Function to generate pandas code from tree of classes
    pandas_statements = []
    # Process incoming SQL file using module
    from pandas_tree import sql_class
    sql_file = sql_class(sql)
    # Flag for using post-aggr output or not
    baseCodeCompHelper = CodeCompilation(sql_file, precise_column_ordering)
    aggrs = ["aggr", "group"]
    treeHelper = TreeHelper()
    
    ccHelper = postorder_traversal(pandas_tree, pandas_statements, baseCodeCompHelper, aggrs, treeHelper)
    
    return pandas_statements, ccHelper

def postorder_traversal(tree, pandas_statements, baseCodeCompHelper, aggrs, treeHelper):
    #if root is None return
    if tree==None:
        return None
    if hasattr(tree, 'nodes'):
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
        
    if hasattr(tree, 'nodes'):
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
            pandas_strings = tree.to_pandas(prev_node_name, df_name, codeCompHelper)
        else:
            pandas_strings = tree.to_pandas(prev_node_name, df_name, codeCompHelper)
        
    else:
        # Add tree.data, the relation name, to codeCompHelper
        codeCompHelper.add_relation(tree.data)
        pandas_strings = tree.to_pandas(tree.data, df_name, codeCompHelper)

   
    for statement in pandas_strings:
        pandas_statements.append(statement)
        
    return codeCompHelper
