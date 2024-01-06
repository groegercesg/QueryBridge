#!/bin/bash

sdqlpy_name="$1"

echo "Running:" $sdqlpy_name

query_base_name=${sdqlpy_name##*/}
query_location=${sdqlpy_name%/*}
pre_pip_name="sdqlpy-compiled-${query_base_name//_/-}"
pip_name="${pre_pip_name%%.*}"

# Uninstall
source SDQLPY/sdqlenv/bin/activate
pip uninstall -y $pip_name

cd $query_location

# Run python script
python3.8 $query_base_name

cd ..

pip uninstall -y $pip_name

echo "Completed:" $sdqlpy_name
