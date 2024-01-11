from enum import Enum

from sdqlpy_classes import *
from sdqlpy_helpers import *

class SupportedOptimisations(Enum):
    V_FOLD = "VerticalFolding"
    COL_ELIM = "ColumnElimination"
    
    @staticmethod
    def has_value(item):
        return item in [v.value for v in SupportedOptimisations.__members__.values()]

def optimisation_runner(sdqlpy_tree, inOpt):
    match inOpt:
        case SupportedOptimisations.V_FOLD:
            opt_v_fold(sdqlpy_tree)
        case SupportedOptimisations.COL_ELIM:
            opt_col_elim(sdqlpy_tree)
        case _:
            raise Exception(f"The optimisation: {inOpt} was not recognised.")
    

def apply_optimisations(sdqlpy_tree, inOptimisations):
    # Check optimisations are all okay
    assert isinstance(inOptimisations, list)
    assert all([x in SupportedOptimisations for x in inOptimisations])
    
    for opt in inOptimisations:
        optimisation_runner(sdqlpy_tree, opt)
        
        
def opt_v_fold(sdqlpy_tree):
    """Folding subsequent operations into each other

    SDQLpyFilterNode -> SDQLpyJoinBuildNode
    SDQLpyJoinNode -> SDQLpyGroupNode
    SDQLpyJoinNode -> SDQLpyJoinBuildNode
    SDQLpyJoinNode -> SDQLpyAggrNode
    
    Notes: 
        - Have to carry over 'output_dict_value_sr_dict' for Query 16
    
    """
    
    pass

def opt_col_elim(sdqlpy_tree):
    pass
    