#!/bin/bash

python3.8 -m venv sdqlenv
source sdqlenv/bin/activate

# Move sdqlpy - have to use a modified version, different configuration from the mainline
cp -r ../../SDQL/sdqlpy/ .
cd sdqlpy/src

python3.8 setup.py build  
python3.8 setup.py install 

python3.8 -m pip install numpy==1.22.0
