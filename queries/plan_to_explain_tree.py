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

class group_aggregate_node(aggregate_node):
    def __init__(self, node_type, parallel_aware, async_capable, output, strategy, partial_mode, parent_relationship, group_key):
        super().__init__(node_type, parallel_aware, async_capable, output, strategy, partial_mode, parent_relationship)
        self.group_key = group_key

class gather_node(base_node):
    def __init__(self, node_type, parallel_aware, async_capable, output, workers_planned, single_copy, parent_relationship):
        super().__init__(node_type, parallel_aware, async_capable, output)
        self.workers_planned = workers_planned
        self.single_copy = single_copy
        self.parent_relationship = parent_relationship
        
class seq_scan_node(base_node):
    def __init__(self, node_type, parallel_aware, async_capable, output, relation_name, schema, alias, parent_relationship, filters):
        super().__init__(node_type, parallel_aware, async_capable, output)
        self.relation_name = relation_name
        self.schema = schema
        self.alias = alias
        self.parent_relationship = parent_relationship
        self.filters = filters

class sort_node(base_node):
    def __init__(self, node_type, parallel_aware, async_capable, output, sort_key, parent_relationship):
        super().__init__(node_type, parallel_aware, async_capable, output)
        self.sort_key = sort_key
        self.parent_relationship = parent_relationship
        
class nested_loop_node(base_node):
    def __init__(self, node_type, parallel_aware, async_capable, output, inner_unique, join_type, parent_relationship):
        super().__init__(node_type, parallel_aware, async_capable, output)
        self.inner_unique = inner_unique
        self.join_type = join_type
        self.parent_relationship = parent_relationship

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
    def __init__(self, node_type, parallel_aware, async_capable, scan_direction, index_name, relation_name, schema, alias, index_cond, filter, output, parent_relationship):
        super().__init__(node_type, parallel_aware, async_capable, output)
        self.parent_relationship = parent_relationship
        self.scan_direction = scan_direction
        self.index_name = index_name
        self.relation_name = relation_name
        self.schema = schema
        self.alias = alias
        self.index_cond = index_cond
        self.filter = filter