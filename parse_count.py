import os
import re
import time
import pandas

def main():
    log_path = "local_perf_3.0.10.log"

    df = pandas.read_csv(log_path, header=0, engine='c', parse_dates=[0])
    print(df[:3])
    print(len(df))

    prev_count = 0  # first record
    insert_count = []
    with open(log_path, "r") as log_file:
        header = log_file.readline()
        print(header)
        for line in log_file:
            line = line.strip()
            # print(line)
            tmp = line.split(",")
            current_count = int(tmp[1])
            insert_count.append( current_count - prev_count)
            prev_count = current_count

    print(len(insert_count))

    df['insert_per_10sec'] = pandas.Series(insert_count, index=df.index)  # set BAND = "NULL"
    print(df[:10])

    df.to_csv("6hr_3_0_10_count.csv")


if __name__ == "__main__":
    main()
