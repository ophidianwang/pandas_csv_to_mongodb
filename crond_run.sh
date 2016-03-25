#!/bin/bash
declare -i minute=$(date +"%M")
declare -i target=${minute}+5
source /home/btserver/pyvenv_jona/bin/activate
python /home/btserver/pandas_csv_to_mongodb/check_speed_multi.py ${target} fm01 fm02 fm03 &
python /home/btserver/pandas_csv_to_mongodb/check_speed_multi.py ${target} fm04 fm05 fm06 &
