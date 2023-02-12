class base_node():
    def __init__(self, node_type, output):
        self.node_type = node_type
        self.output = output
        self.plans = None
        self.parent_relationship = None
        
    def set_plans(self, plans):
        self.plans = plans
        
    def set_parent(self, in_parent):
        self.parent = in_parent
        
    def set_parent_relationship(self, in_parent_relationship):
        self.parent_relationship = in_parent_relationship
        
class projection_node(base_node):
    def __init__(self, node_type, output):
        super().__init__(node_type, output)

class limit_node(base_node):
    def __init__(self, node_type, output):
        super().__init__(node_type, output)
        
class unique_node(base_node):
    def __init__(self, node_type, output):
        super().__init__(node_type, output)

class aggregate_node(base_node):
    def __init__(self, node_type, output):
        super().__init__(node_type, output)
        
    def add_subplan(self, subplan):
        self.subplan_name = subplan

class group_aggregate_node(aggregate_node):
    def __init__(self, node_type, output, group_key):
        super().__init__(node_type, output)
        self.group_key = group_key
            
    def add_filter(self, in_filter):
        self.filter = in_filter
        
class group_node(base_node):
    def __init__(self, node_type, output, group_key):
        super().__init__(node_type, output)
        self.group_key = group_key
            
    def add_filter(self, in_filter):
        self.filter = in_filter

class index_only_scan_node(base_node):
    def __init__(self, node_type, scan_direction, index_name, relation_name, schema, alias, output):
        super().__init__(node_type, output)
        self.scan_direction = scan_direction
        self.index_name = index_name
        self.relation_name = relation_name
        self.schema = schema
        self.alias = alias
        
    def add_filter(self, in_filter):
        self.filter = in_filter

class gather_node(base_node):
    def __init__(self, node_type, output, workers_planned, single_copy):
        super().__init__(node_type, output)
        self.workers_planned = workers_planned
        self.single_copy = single_copy

class bitmap_heap_scan_node(base_node):
    def __init__(self, node_type, output, relation_name, schema, alias, recheck_cond):
        super().__init__(node_type, output)
        self.relation_name = relation_name
        self.schema = schema
        self.alias = alias
        self.recheck_cond = recheck_cond

class seq_scan_node(base_node):
    def __init__(self, node_type, output, relation_name, alias):
        super().__init__(node_type, output)
        self.relation_name = relation_name
        self.alias = alias
        
    def add_subplan_name(self, in_name):
        self.subplan_name = in_name
        
    def add_parent_relationship(self, parent):
        self.parent_relationship = parent
        
    def add_filters(self, in_filters):
        self.filters = in_filters

class sort_node(base_node):
    def __init__(self, node_type, output, sort_key):
        super().__init__(node_type, output)
        self.sort_key = sort_key

class subquery_scan_node(base_node):
    def __init__(self, node_type, output, alias):
        super().__init__(node_type, output)
        self.alias = alias
        
class nested_loop_node(base_node):
    def __init__(self, node_type, output, inner_unique, join_type):
        super().__init__(node_type, output)
        self.inner_unique = inner_unique
        self.join_type = join_type
        
    def add_merge_cond(self, merge_cond):
        self.merge_cond = merge_cond
        
    def add_filter(self, in_filters):
        self.filter = in_filters

class hash_join_node(base_node):
    def __init__(self, node_type, output, inner_unique, join_type, hash_cond):
        super().__init__(node_type, output)
        self.inner_unique = inner_unique
        self.join_type = join_type
        self.hash_cond = hash_cond
    
    def add_filter(self, in_filter):
        self.filter = in_filter
        
class hash_node(base_node):
    def __init__(self, node_type, output):
        super().__init__(node_type, output)
        
class index_scan_node(base_node):
    def __init__(self, node_type, scan_direction, index_name, relation_name, alias, output):
        super().__init__(node_type, output)
        self.scan_direction = scan_direction
        self.index_name = index_name
        self.relation_name = relation_name
        self.alias = alias
        
    def add_index_cond(self, cond):
        self.index_cond = cond
        
    def add_filter(self, in_filter):
        self.filter = in_filter
        
class bitmap_index_scan_node(base_node):
    def __init__(self, node_type, index_name):
        super().__init__(node_type, None)
        self.index_name = index_name
        
    def add_index_cond(self, cond):
        self.index_cond = cond

class incremental_sort_node(base_node):
    def __init__(self, node_type, output, sort_key, presorted_key):
        super().__init__(node_type, output)
        self.sort_key = sort_key
        self.presorted_key = presorted_key
        
class merge_join_node(base_node):
    def __init__(self, node_type, output, inner_unique, join_type, merge_cond):
        super().__init__(node_type, output)
        self.inner_unique = inner_unique
        self.join_type = join_type
        self.merge_cond = merge_cond
        
    def add_filter(self, in_filter):
        self.filter = in_filter
        
class materialize_node(base_node):
    def __init__(self, node_type, output):
        super().__init__(node_type, output)

