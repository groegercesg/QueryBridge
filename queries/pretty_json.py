import json
output_file = "results/14/14_explain.json"

f = open(output_file)   
  
# returns JSON object as a dictionary
explain_json = json.load(f)[0]
f.close()

print(json.dumps(explain_json, indent=4))
