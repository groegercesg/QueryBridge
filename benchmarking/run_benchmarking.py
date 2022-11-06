import argparse
import sys
import json
import subprocess
from pathlib import Path
import shutil

def init_argparse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        usage="%(prog)s [OPTION] [FILE]...",
        description="Benchmark SQL and Pandas queries, based on instructions from the manifest file."
    )
    parser.add_argument(
        "-v", "--version", action="version",
        version = f"{parser.prog} version 0.0.1"
    )
    parser.add_argument('--file',
                       metavar='file',
                       type=str,
                       help='The file contain instructions for our testing')
    
    return parser

def main():
    parser = init_argparse()
    args = parser.parse_args()
    
    if len(sys.argv)==1:
        # display help message when no args are passed.
        parser.print_help()
        sys.exit(1)
        
    manifest = args.file
    manifest_json = json.load(open(manifest))
    
    
    print(manifest_json['Test Name'])
    
    # We will run the converter to create our pandas files from our sql files
    # We want this to output the converted queries to a given directory
    # manifest_json["Temporary Directory"]
    
    # Delete if already exists
    temp_path = Path(manifest_json["Temporary Directory"])
    if temp_path.exists() and temp_path.is_dir():
        shutil.rmtree(temp_path)
    # Make a folder
    Path(temp_path).mkdir(parents=True, exist_ok=True)
    
    # Run converter
    cmd = ["python3", manifest_json["SQL Converter Location"], '--file', manifest_json["SQL Queries"][0]["Query Location"], '--benchmarking', "True", "--output_location", manifest_json["Temporary Directory"], "--name", manifest_json["SQL Queries"][0]["Pandas Name"]]    
    
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    
    if result.returncode != 0:
        print(result.stdout)
        print(result.stderr)
        raise Exception( f'Invalid result: { result.returncode }' )
    
    
    # Tear Down
    # Delete temporary folder
        
if __name__ == "__main__":
    main()
