import os
import glob
import re
import subprocess
from subprocess import PIPE
import shutil

class data_generator():
    def __init__(self, data_dir, dbgen_path, scaling_factor = 1):
        self.scaling_factor = float(scaling_factor)
        self.data_dir = data_dir
        self.dbgen_path = dbgen_path
        
        if self.dbgen_path == None:
            raise Exception("Please specify a location of a dbgen generator.")
        
        # Run the prep data method
        self.prepare_data()

    def prepare_data(self):
        # Prepare Data
            # Build DBGEN
        if self.build_dbgen():
            print("Could not build the dbgen.")
            exit(1)
        print("Built dbgen from source.")
            # Generate Data
        # try to generate data files
        if self.generate_data():
            print("Could not generate data files.")
            exit(1)
        print("Created data files in %s" % self.data_dir)
        
    def generate_data(self):
        """
        Generates the data from dbgen, and moves it into the data_dir directory.
        Return:
            0 if successful
            non zero otherwise
        """
        cur_dir = os.getcwd()
        
        # Change into the dbgen dir
        os.chdir(self.dbgen_path)
        
        # Delete all tbl files in folder
        for in_fname in glob.glob("*.tbl"):
            os.remove(in_fname)
        
        p = subprocess.Popen([os.path.join(".", "dbgen"), "-vf", "-s", str(self.scaling_factor)], stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
        
        if not p.returncode:
            # We have generated the data files successfully, but they're all in the dbgen_path
            # We should move them to self.data_dir
                try:
                    # TODO: This is hardcoded and sloppy, but it works for now
                    # Check if directory exists
                    if os.path.isdir("../" + self.data_dir):
                        # Delete it
                        shutil.rmtree("../" + self.data_dir)
                        
                    # Remake it
                    os.makedirs("../" + self.data_dir)
                    # Match anything in self.dbgen_path with an extension of .tbl
                    for in_fname in glob.glob("*.tbl"):
                        # Change the file permissions
                        os.chmod(in_fname, 0o777)
                        fname = os.path.basename(in_fname)
                        out_fname = os.path.join("../" + self.data_dir, fname + ".csv")
                        try:
                            with open(in_fname) as in_file, open(out_fname, "w") as out_file:
                                for inline in in_file:
                                    outline = re.sub("\|$", "", inline)
                                    out_file.write(outline)
                            os.remove(in_fname)
                        except IOError as e:
                            print("Something bad happened while transforming data files. (%s)" % e)
                            # Change back out of it
                            os.chdir(cur_dir)
                            return 1
                except IOError as e:
                    print("Unable to create data directory %s. (%s)" % (self.data_dir, e))
                    # Change back out of it
                    os.chdir(cur_dir)
                    return 1
                
                # All files written successfully. Return success code.
                # Change back out of it
                os.chdir(cur_dir)
                return 0
        else:
            
            # Change back out of it
            os.chdir(cur_dir)
            print(out)
            print(err)
            
            # TODO:
            # Sometimes we get here, because a file is locked, permission issues
            # We can resolve this by deleting all the "*.tbl" files in self.dbgen_path
            # And rerunning this current function
            return p.returncode

    def build_dbgen(self):
        """
        Compiles the dbgen from source.
        The Makefile must be present in the same directory as this script.
        Return:
            0 if successful
            non zero otherwise
        """
        cur_dir = os.getcwd()
        
        # Change into the dbgen dir
        os.chdir(self.dbgen_path)
        
        p = subprocess.Popen(["make", "-f", os.path.join(cur_dir, "makefile")], stdout=PIPE, stderr=PIPE)
        out, err = p.communicate()
        
        # Change back out of it
        os.chdir(cur_dir)
        
        if not p.returncode:
            return p.returncode
        else:
            print(out)
            print(err)
            return p.returncode
