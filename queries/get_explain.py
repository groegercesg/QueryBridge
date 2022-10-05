import json
import subprocess

file = "6_explain.sql"
output_file = "explain.json"
command = "psql -d tpchdb -U tpch -a -f " + file + " > " + output_file

# Run command
subprocess.getoutput(command)

# Clean-up output_file, make into nice JSON and load

# Remove before "------"
with open(output_file, 'r+') as fp:
    # read an store all lines into list
    lines = fp.readlines()
    # move file pointer to the beginning of a file
    fp.seek(0)
    # truncate the file
    fp.truncate()
    
    start_line = 0
    for i, line in enumerate(lines):
        if "----------------------------------" in line:
            start_line = i
            break
    
    # after start line
    # E.g.: lines[1:] from line 2 to last line
    # And trim the final few lines
    lines = lines[start_line+1:len(lines)-2]
    
    # Remove "+" from lines
    for i in range(len(lines)):
        lines[i] = lines[i][:-2] + '\n'
        lines[i] = lines[i].strip()

    # Append bracket to last line
    lines[len(lines) - 1] = lines[len(lines) - 1] + "]"

    # start writing lines 
    fp.writelines(lines)
    
# Load json from written file
explain_json = ""
f = open(output_file)
  
# returns JSON object as a dictionary
explain_json = json.load(f)[0]
f.close()

#print(explain_json)
print(json.dumps(explain_json, indent=4))