def get_class_name(node):
    return str(str(node.__class__.__name__).split("_")[0])

def make_pandas(pandas_list):
    # Function to generate pandas code from tree of classes
    pandas_statements = []
    
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
            
            # Check for limit last
            #if prev_class_name == "aggr" and class_name == "limit" and i == len(pandas_list) - 1:
            #    # We are in the situation of penultimate aggr and final limit, let's skip the limit
            #    # Add in a print for the final value
            #    pandas_strings = ["print(" + str(pandas_statements[-1].split()[0]) + ")"]
            #else:
            pandas_strings = node.to_pandas("df_"+prev_class_name, "df_"+class_name)
            
        for statement in pandas_strings:
            pandas_statements.append(statement)
    
    return pandas_statements
