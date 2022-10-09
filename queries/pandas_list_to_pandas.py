def get_class_name(node):
    return str(str(node.__class__.__name__).split("_")[0])

def make_pandas(pandas_list):
    # Function to generate pandas code from tree of classes
    pandas_statements = []
    
    # Break for aggr
    aggr_break = False
    
    for i, node in enumerate(pandas_list):
        # Special break for aggr statements
        if aggr_break:
            break
        
        class_name = get_class_name(node)
        
        statement_string = ""
        
        # We need to find handle two cases
        # Either we have a previous statement, and so need to refer to it
        # Or we don't so, need to refer to the previous as DF
        if pandas_statements == []:
            # First operation to modify DF
            statement_string += "df_" + class_name + " = df"
            
            pandas_string, output = node.to_pandas("df")
            
            if class_name == "aggr":
                # If it's an aggr we don't have the "df stuff", and this is the final statement
                statement_string += pandas_string
                # Also, if it's an aggr, then it's the final statement
                aggr_break = True
            else:
                statement_string += pandas_string
            
        else:
            # Not first time, figure out what previous df would be called
            prev_class_name = get_class_name(pandas_list[i-1])
            pandas_string, output = node.to_pandas("df_"+prev_class_name)
            
            if class_name == "aggr":
                # If it's an aggr we don't have the "df stuff", and this is the final statement
                statement_string += pandas_string
                # Also, if it's an aggr, then it's the final statement
                aggr_break = True
            else:
                statement_string += "df_" + class_name + " = df_" + prev_class_name
                statement_string += pandas_string
                
        pandas_statements.append(statement_string)
        
        # We are about to stop iterating
        # This means we have broken prematurely on a aggr
        # So we must have a output term we are required to add
        if aggr_break:
            pandas_statements.append("print("+output+")")
    
    return pandas_statements
