def make_pandas(explain_tree):
    # Function to generate pandas code from tree of classes
    
    children = ""
    if explain_tree.plans is not None:
        # There are things below, run this function on them
        children = make_pandas(explain_tree.plans)
        
    # Process the current node
    current_code = ""
    if explain_tree.node_type == "Seq Scan":
        current_code = seq_scan_pandas(explain_tree)
    elif explain_tree.node_type == "Aggregate":
        current_code = aggregate_pandas(explain_tree)
    else:
        current_code = explain_tree.node_type
    
    return current_code + " || " + children


def aggregate_pandas(node):
    if node.partial_mode == "Partial":
        print(node.output[0].split("PARTIAL ")[1])
    
    return node.node_type
        
        
def seq_scan_pandas(node):    
    # Replace AND with & and convert to string
    filters = str(node.filters.replace("AND", "&"))
    # Remove first and last brackets
    filters = filters[1:-1]
    
    import re
    regex = re.compile(r"::\w+(\s+\w+)*\)", flags=re.MULTILINE)
    
    # Iterate through filters, remove strings that start with ::
    line_split = filters.split("&")
    for i in range(len(line_split)):
        regex_search = regex.search(line_split[i])
        if regex_search != None:
            # This provides information about the datatype the comparator should be
            # print(str(regex_search.group())[2:-1])
            pass
        line_split[i] = regex.sub(")", line_split[i])
    
    filters = "&".join(line_split)
    
    return "DF_filt = DF["+filters+"]"
    