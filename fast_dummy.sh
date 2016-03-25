#!/bin/bash
source /home/btserver/pyvenv_jona/bin/activate
python /home/btserver/pandas_csv_to_mongodb/fast_dummy.py 1 &
python /home/btserver/pandas_csv_to_mongodb/fast_dummy.py 2 &
python /home/btserver/pandas_csv_to_mongodb/fast_dummy.py 3 &