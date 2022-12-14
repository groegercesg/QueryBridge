import os
import shutil
import subprocess

# CLASSES

class Constants_class():
    def __init__(self, inputs, outputs, name, converter, output_name):
        self.INPUTS_DIR = inputs
        self.OUTPUTS_DIR = outputs
        self.QUERY_NAME = name
        self.CONVERTER_LOC = converter
        self.OUTPUT_NAME = output_name


# FUNCTIONS
def write_to_file(filepath, content):
    f = open(filepath, "w")
    f.write(content)
    f.close()
    
def read_from_file(filepath):
    f = open(filepath, "r")
    return f.read()

def remove_dir(folder):
    if os.path.exists(folder) and os.path.isdir(folder):
        shutil.rmtree(folder)
    
def delete_files_in_dir(folder):
    for filename in os.listdir(folder):
        file_path = os.path.join(folder, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print('Failed to delete %s. Reason: %s' % (file_path, e))

def count_files_in_directory(folder):
    _, _, files = next(os.walk(folder))
    return len(files)

def run_query(sql, constants):
    # Write to a file
    write_to_file(constants.INPUTS_DIR+constants.QUERY_NAME, sql)  
        
    # Run this query
    cmd = ["python3", constants.CONVERTER_LOC, '--file', constants.INPUTS_DIR+constants.QUERY_NAME, "--output_location", constants.OUTPUTS_DIR, '--benchmarking', "False", "--name", constants.OUTPUT_NAME]    
    
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    
    if result.returncode != 0:
        print(result.stdout)
        print(result.stderr)
        raise Exception( f'Invalid result: { result.returncode }' )
    
    return str(read_from_file(constants.OUTPUTS_DIR+constants.OUTPUT_NAME)).strip()