import pydot

def get_class_id(node):
    return str(id(node))

def get_class_name(node):
    # Capitalise first letter
    result = str(str(node.__class__.__name__).split("_")[0]).capitalize()
    if result == "Aggr":
        result = "Aggregate"
    return result

# color of nodes, format is:
#   fillcolor, fontcolor
pandas_node_colors = {
    "Limit": ["#FD7979", "black"],
    "Sort": ["#7FC6C6", "black"],
    "Aggregate": ["#CCFF3F", "black"],
    "Group": ["#FFD23F", "black"],
    "Merge": ["#CCC9E7", "black"],
    "Filter": ["#FFA175", "black"],
    "Rename": ["#75ffa1", "black"],
    "DATA": ["#394648", "white"]
}

def walk_pandas_tree(graph, class_tree, parent_node=None):
    """
    Plotting function for the tree of our class
    """

    if parent_node is not None:

        from_name = get_class_name(parent_node).replace("\"", "") + '_' + str(get_class_name(class_tree)) + '_' + str(get_class_id(class_tree)) # unique name
        from_label = str(get_class_name(class_tree))

        # Choose colour
        if pandas_node_colors.get(from_label, None) == None:
            # Default
            edge_color, font_color = "white", "black"
        else:
            edge_color, font_color = pandas_node_colors.get(from_label, None)
        
        node_from = pydot.Node(from_name, label=from_label, fontname="Helvetica", fontcolor=font_color, style="filled", fillcolor=edge_color)
        graph.add_node(node_from)
        graph.add_edge(pydot.Edge(parent_node, node_from) )

        if hasattr(class_tree, "nodes"):
            if class_tree.nodes is not None: # if more leaves below this node
                for plan in class_tree.nodes:
                    walk_pandas_tree(graph, plan, node_from)

        else: # if leaf node
            to_name = str(get_class_name(class_tree)) + '_' + str(class_tree.data) # unique name
            to_label = str(class_tree.data)
            
            # Choose colour
            if pandas_node_colors.get("DATA", None) == None:
                # Default
                edge_color, font_color = "white", "black"
            else:
                edge_color, font_color = pandas_node_colors.get("DATA", None)

            node_to = pydot.Node(to_name, label=to_label, shape='box', fontname="Helvetica", fontcolor=font_color, style="filled", fillcolor=edge_color)
            graph.add_node(node_to)
            graph.add_edge(pydot.Edge(node_from, node_to))

    else:
        # Root
        from_name =  str(get_class_name(class_tree))
        from_label = str(get_class_name(class_tree))
        
        # Choose colour
        if pandas_node_colors.get(from_label, None) == None:
            # Default
            edge_color, font_color = "white", "black"
        else:
            edge_color, font_color = pandas_node_colors.get(from_label, None)

        node_from = pydot.Node(from_name, label=from_label, fontname="Helvetica", fontcolor=font_color, style="filled", fillcolor=edge_color)
        graph.add_node(node_from)
        if hasattr(class_tree, "nodes"):
            if class_tree.nodes is not None: # if more leaves below this node
                for plan in class_tree.nodes:
                    walk_pandas_tree(graph, plan, node_from)

def walk_class_tree(graph, class_tree, parent_node=None):
    """
    Plotting function for the tree of our class
    """

    if parent_node is not None:

        from_name = parent_node.get_name().replace("\"", "") + '_' + str(class_tree.node_type) + '_' + str(get_class_id(class_tree)) # unique name
        from_label = str(class_tree.node_type)

        node_from = pydot.Node(from_name, label=from_label)
        graph.add_node(node_from)
        graph.add_edge(pydot.Edge(parent_node, node_from) )

        if class_tree.plans is not None: # if more leaves below this node
            for plan in class_tree.plans:
                walk_class_tree(graph, plan, node_from)

        else: # if leaf node
            if hasattr(class_tree, "relation_name"):
                to_name = str(class_tree.node_type) + '_' + str(class_tree.relation_name) # unique name
                to_label = str(class_tree.relation_name)
            else:
                to_name = str(class_tree.node_type)
                to_label = ""

            node_to = pydot.Node(to_name, label=to_label, shape='box')
            graph.add_node(node_to)
            graph.add_edge(pydot.Edge(node_from, node_to))

    else:
        # Root
        from_name =  str(class_tree.node_type)
        from_label = str(class_tree.node_type)

        node_from = pydot.Node(from_name, label=from_label)
        graph.add_node(node_from)
        if class_tree.plans is not None:
            for plan in class_tree.plans:
                walk_class_tree(graph, plan, node_from)

def plot_tree(tree, name):
    
    # first we create a new graph, we do that with pydot.Dot()
    graph = pydot.Dot(graph_type='graph')

    walk_class_tree(graph, tree)

    graph.write_pdf(name+'.pdf')
    
def plot_pandas_tree(tree, name):
    
    # first we create a new graph, we do that with pydot.Dot()
    graph = pydot.Dot(graph_type='graph')

    walk_pandas_tree(graph, tree)

    graph.write_pdf(name+'.pdf')