class CodeCompilation():
    def __init__(self):
        self.usePostAggr = False
        self.indexes = []
        self.relations = []
        
    def setAggr(self, aggr):
        self.usePostAggr = aggr
        
    def setIndexes(self, idxs):
        self.indexes = idxs
        
    def add_relation(self, relation):
        self.relations.append(relation)

def get_class_name(node):
    return str(str(node.__class__.__name__).split("_")[0])

def make_pandas(pandas_tree):
    # Function to generate pandas code from tree of classes
    pandas_statements = []
    # Flag for using post-aggr output or not
    codeCompHelper = CodeCompilation()
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

    # Process the node we're at
    class_name = get_class_name(tree)
    if hasattr(tree, 'nodes'):
        # Not first time, figure out what previous df would be called
        prev_class_names = []
        for prev_node in tree.nodes:
            prev_class_names.append(get_class_name(prev_node))
        # Process prev_class_names
        if len(prev_class_names) == 1:
            prev_class_name = prev_class_names[0]
        else:
            # We have to handle multiple classes below, this must be a MERGE!
            # We pass in a list of previous names, 
            prev_class_name = prev_class_names
            
        # Decide on what output to use
        if class_name in aggrs:
            # We are in the aggr
            codeCompHelper.setAggr(True)
            pandas_strings = tree.to_pandas(prev_class_name, class_name, codeCompHelper)
        else:
            pandas_strings = tree.to_pandas(prev_class_name, class_name, codeCompHelper)
        
    else:
        # Add tree.data, the relation name, to codeCompHelper
        codeCompHelper.add_relation(tree.data)
        pandas_strings = tree.to_pandas(tree.data, class_name, codeCompHelper)

   
    for statement in pandas_strings:
        pandas_statements.append(statement)
