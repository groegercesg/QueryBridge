from enum import Enum

from universal_plan_nodes import *

class SupportedOptimisations(Enum):
    COL_ELIM = "ColumnElimination"
    
    @staticmethod
    def has_value(item):
        return item in [v.value for v in SupportedOptimisations.__members__.values()]

OPTIMISATIONS_ORDER = [
    SupportedOptimisations.COL_ELIM
]

def optimisation_runner(uplan_tree, inOpt):
    match inOpt:
        case SupportedOptimisations.COL_ELIM.value:
            uplan_tree = opt_col_elim(uplan_tree)
        case _:
            raise Exception(f"The optimisation: {inOpt} was not recognised.")
        
    return uplan_tree
    

def uplan_apply_optimisations(uplan_tree, inOptimisations):
    if inOptimisations == [""]:
        return uplan_tree
    
    # Check optimisations are all okay
    assert isinstance(inOptimisations, list)
    assert all([SupportedOptimisations.has_value(x) for x in inOptimisations])
    assert len(inOptimisations) <= len(OPTIMISATIONS_ORDER)
    
    # We should sort the optimisations
    order = {v.value:i for i,v in enumerate(OPTIMISATIONS_ORDER)}
    inOptimisations = sorted(inOptimisations, key=lambda x: order[x])
    
    for opt in inOptimisations:
        uplan_tree = optimisation_runner(uplan_tree, opt)
        
    return uplan_tree

def opt_col_elim(uplan_tree):
    pass
    return uplan_tree
    