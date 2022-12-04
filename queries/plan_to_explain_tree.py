class base_node():
    def __init__(self, node_type, parallel_aware, async_capable, output):
        self.node_type = node_type
        self.parallel_aware = parallel_aware
        self.async_capable = async_capable
        self.output = output
        self.plans = None
        
    def set_plans(self, plans):
        self.plans = plans

class limit_node(base_node):
    def __init__(self, node_type, parallel_aware, async_capable, output):
        super().__init__(node_type, parallel_aware, async_capable, output)
        
class aggregate_node(base_node):
    def __init__(self, node_type, parallel_aware, async_capable, output, strategy, partial_mode, parent_relationship):
        super().__init__(node_type, parallel_aware, async_capable, output)
        self.strategy = strategy
        self.partial_mode = partial_mode
        self.parent_relationship = parent_relationship
        
    def add_subplan(self, subplan):
        self.subplan_name = subplan

class group_aggregate_node(aggregate_node):
    def __init__(self, node_type, parallel_aware, async_capable, output, strategy, partial_mode, parent_relationship, group_key):
        super().__init__(node_type, parallel_aware, async_capable, output, strategy, partial_mode, parent_relationship)
        self.group_key = group_key
            
    def add_filter(self, in_filter):
        self.filter = in_filter

class index_only_scan_node(base_node):
    def __init__(self, node_type, parallel_aware, async_capable, scan_direction, index_name, relation_name, schema, alias, output, filter, parent_relationship):
        super().__init__(node_type, parallel_aware, async_capable, output)
        self.scan_direction = scan_direction
        self.index_name = index_name
        self.relation_name = relation_name
        self.schema = schema
        self.alias = alias
        self.filter = filter
        self.parent_relationship = parent_relationship

class gather_node(base_node):
    def __init__(self, node_type, parallel_aware, async_capable, output, workers_planned, single_copy, parent_relationship):
        super().__init__(node_type, parallel_aware, async_capable, output)
        self.workers_planned = workers_planned
        self.single_copy = single_copy
        self.parent_relationship = parent_relationship

class bitmap_heap_scan_node(base_node):
    def __init__(self, node_type, parallel_aware, async_capable, output, relation_name, schema, alias, recheck_cond):
        super().__init__(node_type, parallel_aware, async_capable, output)
        self.relation_name = relation_name
        self.schema = schema
        self.alias = alias
        self.recheck_cond = recheck_cond
        
    def add_parent_relationship(self, parent):
        
        self.parent_relationship = parent

class seq_scan_node(base_node):
    def __init__(self, node_type, parallel_aware, async_capable, output, relation_name, schema, alias):
        super().__init__(node_type, parallel_aware, async_capable, output)
        self.relation_name = relation_name
        self.schema = schema
        self.alias = alias
        
    def add_subplan_name(self, in_name):
        self.subplan_name = in_name
        
    def add_parent_relationship(self, parent):
        self.parent_relationship = parent
        
    def add_filters(self, in_filters):
        self.filters = in_filters

class sort_node(base_node):
    def __init__(self, node_type, parallel_aware, async_capable, output, sort_key, parent_relationship):
        super().__init__(node_type, parallel_aware, async_capable, output)
        self.sort_key = sort_key
        self.parent_relationship = parent_relationship

class subquery_scan_node(base_node):
    def __init__(self, node_type, parallel_aware, async_capable, output, alias, parent_relationship):
        super().__init__(node_type, parallel_aware, async_capable, output)
        self.alias = alias
        self.parent_relationship = parent_relationship
        
class nested_loop_node(base_node):
    def __init__(self, node_type, parallel_aware, async_capable, output, inner_unique, join_type, parent_relationship):
        super().__init__(node_type, parallel_aware, async_capable, output)
        self.inner_unique = inner_unique
        self.join_type = join_type
        self.parent_relationship = parent_relationship
        
    def add_merge_cond(self, merge_cond):
        self.merge_cond = merge_cond
        
    def add_filter(self, in_filters):
        self.filter = in_filters

class hash_join_node(base_node):
    def __init__(self, node_type, parallel_aware, async_capable, output, inner_unique, join_type, hash_cond, parent_relationship):
        super().__init__(node_type, parallel_aware, async_capable, output)
        self.inner_unique = inner_unique
        self.join_type = join_type
        self.hash_cond = hash_cond
        self.parent_relationship = parent_relationship
        
class hash_node(base_node):
    def __init__(self, node_type, parallel_aware, async_capable, output, parent_relationship):
        super().__init__(node_type, parallel_aware, async_capable, output)
        self.parent_relationship = parent_relationship
        
class index_scan_node(base_node):
    def __init__(self, node_type, parallel_aware, async_capable, scan_direction, index_name, relation_name, schema, alias, output, parent_relationship):
        super().__init__(node_type, parallel_aware, async_capable, output)
        self.parent_relationship = parent_relationship
        self.scan_direction = scan_direction
        self.index_name = index_name
        self.relation_name = relation_name
        self.schema = schema
        self.alias = alias
        
    def add_index_cond(self, cond):
        self.index_cond = cond
        
    def add_filter(self, in_filter):
        self.filter = in_filter
        
class bitmap_index_scan_node(base_node):
    def __init__(self, node_type, parallel_aware, async_capable, index_name, parent_relationship):
        super().__init__(node_type, parallel_aware, async_capable, None)
        self.parent_relationship = parent_relationship
        self.index_name = index_name
        
    def add_index_cond(self, cond):
        self.index_cond = cond

class incremental_sort_node(base_node):
    def __init__(self, node_type, parallel_aware, async_capable, output, parent_relationship, sort_key, presorted_key):
        super().__init__(node_type, parallel_aware, async_capable, output)
        self.parent_relationship = parent_relationship
        self.sort_key = sort_key
        self.presorted_key = presorted_key
        
class merge_join_node(base_node):
    def __init__(self, node_type, parallel_aware, async_capable, output, inner_unique, join_type, merge_cond, parent_relationship):
        super().__init__(node_type, parallel_aware, async_capable, output)
        self.inner_unique = inner_unique
        self.join_type = join_type
        self.merge_cond = merge_cond
        self.parent_relationship = parent_relationship
        
class materialize_node(base_node):
    def __init__(self, node_type, parallel_aware, async_capable, output, parent_relationship):
        super().__init__(node_type, parallel_aware, async_capable, output)
        self.parent_relationship = parent_relationship

