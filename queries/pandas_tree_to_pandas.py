from collections import defaultdict

class CodeCompilation():
    def __init__(self, sql_class):
        self.usePostAggr = False
        self.indexes = []
        self.relations = []
        self.sql = sql_class
        # Dictionary to track how many "filter" nodes there have been
        self.node_type_tracker = defaultdict(int)
        # Dictionary to map class: filter_node (id: 123123) to df_name: df_filter_2
        self.node_id_tracker = {}
        
    def setAggr(self, aggr):
        self.usePostAggr = aggr
        
    def setIndexes(self, idxs):
        self.indexes = idxs
        
    def add_relation(self, relation):
        self.relations.append(relation)

def get_class_name(node):
    return str(str(node.__class__.__name__).split("_")[0])

def get_class_id(node):
    return str(id(node))

def make_pandas(pandas_tree, sql):
    # Function to generate pandas code from tree of classes
    pandas_statements = []
    # Process incoming SQL file using module
    from pandas_tree import sql_class
    sql_file = sql_class(sql)
    # Flag for using post-aggr output or not
    codeCompHelper = CodeCompilation(sql_file)
    aggrs = ["aggr", "group"]
    
    postorder_traversal(pandas_tree, pandas_statements, codeCompHelper, aggrs)
    
    return pandas_statements

def postorder_traversal(tree, pandas_statements, codeCompHelper, aggrs):
    #if root is None return
    if tree==None:
        return
    if hasattr(tree, 'nodes'):
        for node in tree.nodes:
            #traverse left subtree
            postorder_traversal(node, pandas_statements, codeCompHelper, aggrs)

    # Process the node we're at, create the df_name for it
    current_class_name = get_class_name(tree)
    df_name = "df_" + current_class_name + "_" + str(codeCompHelper.node_type_tracker.get(current_class_name, 0) + 1)
    # Increment current class in node_type_tracker
    codeCompHelper.node_type_tracker[current_class_name] += 1
    # Store id and df_name in node_id_tracker
    current_class_id = get_class_id(tree)
    if codeCompHelper.node_id_tracker.get(current_class_id, None) == None:
        codeCompHelper.node_id_tracker[current_class_id] = df_name
    else:
        # We have an id collision, these are not unique
        raise ValueError("We have a node_id collision!")
        
    if hasattr(tree, 'nodes'):
        # Not first time, figure out what previous df would be called
        prev_node_names = []
        for prev_node in tree.nodes:
            prev_node_id = get_class_id(prev_node)
            prev_node_names.append(codeCompHelper.node_id_tracker[prev_node_id])
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
