import pandas_tree
import pandas_tree_to_pandas

class content():
    def __init__(self, output):
        self.output = output

target_string = ["CURRENT_DF['sumo_totalprice'] = [(PREV_DF.o_totalprice).sum()]"]
in_string = ["sum(o_totalprice)"]
in_class = content(in_string)
ccHelper = pandas_tree_to_pandas.CodeCompilation("", False, False)
out_string = pandas_tree.do_aggregation(in_class, "PREV_DF", "CURRENT_DF", ccHelper)

print("Out String:")
print(out_string)
print("Target String:")
print(target_string)