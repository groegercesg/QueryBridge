def get_class_name(node):
    return str(str(node.__class__.__name__).split("_")[0])

def make_pandas(pandas_list):
    # Function to generate pandas code from tree of classes
    pandas_statements = []
    # Flag for using post-aggr output or not
    usePostAggr = False
    aggrs = ["aggr", "group"]
    
    for i, node in enumerate(pandas_list):        
        class_name = get_class_name(node)
                
        # We need to find handle two cases
        # Either we have a previous statement, and so need to refer to it
        # Or we don't so, need to refer to the previous as DF
        if pandas_statements == []:
            # First operation to modify DF
            pandas_strings = node.to_pandas("df", "df_"+class_name)            
        else:           
            # Not first time, figure out what previous df would be called
            prev_class_name = get_class_name(pandas_list[i-1])
            
            print("postAggr")
            
            # Decide on what output to use
            if class_name in aggrs:
                # We are in the aggr
                usePostAggr = True
                pandas_strings = node.to_pandas("df_"+prev_class_name, "df_"+class_name, usePostAggr)
                
                print("We are post_aggr")
            else:
                pandas_strings = node.to_pandas("df_"+prev_class_name, "df_"+class_name, usePostAggr)
            
            
            
        for statement in pandas_strings:
            pandas_statements.append(statement)
    
    return pandas_statements
