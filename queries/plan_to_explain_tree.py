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

