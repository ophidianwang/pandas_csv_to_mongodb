#!/bin/bash
source /home/btserver/pyvenv_jona/bin/activate
python /home/btserver/pandas_csv_to_mongodb/one_day_2.py fm01 fm02 &
python /home/btserver/pandas_csv_to_mongodb/one_day_2.py fm03 fm04 &
python /home/btserver/pandas_csv_to_mongodb/one_day_2.py fm05 fm06 &

