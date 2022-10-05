import pydot

def walk_class_tree(graph, class_tree, parent_node=None):
    '''
    Plotting function for the tree of our class
    '''

    if parent_node is not None:

        from_name = parent_node.get_name().replace("\"", "") + '_' + str(class_tree.node_type)
        from_label = str(class_tree.node_type)

        node_from = pydot.Node(from_name, label=from_label)
        graph.add_node(node_from)
        graph.add_edge( pydot.Edge(parent_node, node_from) )

        if class_tree.plans is not None: # if more leaves below this node

            walk_class_tree(graph, class_tree.plans, node_from)

        else: # if leaf node
            to_name = str(class_tree.node_type) + '_' + str(class_tree.relation_name) # unique name
            to_label = str(class_tree.relation_name)

            node_to = pydot.Node(to_name, label=to_label, shape='box')
            graph.add_node(node_to)
            graph.add_edge(pydot.Edge(node_from, node_to))

    else:

        from_name =  str(class_tree.node_type)
        from_label = str(class_tree.node_type)

        node_from = pydot.Node(from_name, label=from_label)
        graph.add_node(node_from)
        walk_class_tree(graph, class_tree.plans, node_from)

def plot_tree(tree, name):
    
    # first you create a new graph, you do that with pydot.Dot()
    graph = pydot.Dot(graph_type='graph')

    walk_class_tree(graph, tree)

    graph.write_png(name+'.png')