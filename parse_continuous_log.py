import os
import re
import time
import pandas

def parse_log_to_csv(log_dir, csv_path):
    # spend time
    timestamp_map = {}
    datetime_map = {}
    for i in xrange(217,220):
        timestamp_map[str(i)] = {"123":[], "456":[]}
        datetime_map[str(i)] = {"123":[], "456":[]}

    for dir_name in log_dir:
        file_list = os.listdir(dir_name)
        tmp = dir_name.split("_")
        dir_postfix = tmp[1]
        for filename in file_list:
            tmp = filename.split("_")
            tmp = tmp[2].split(".")
            file_postfix = tmp[0]
            file_path = dir_name + "/" + filename
            with open(file_path, "r") as file_file:
                for line in file_file:
                    line = line.strip()
                    if re.match(r"process.*", line) or re.match(r"after.*", line):
                        tmp = line.split("@ ")
                        datetime_map[dir_postfix][file_postfix].append(tmp[1])
                        time_tuple = time.strptime(tmp[1], "%Y-%m-%d %H:%M:%S")
                        timestamp_map[dir_postfix][file_postfix].append(time.mktime(time_tuple))

    df = pandas.DataFrame({
        "TIMESTAMP_217_A":pandas.Series(timestamp_map["217"]["123"]),
        "TIMESTAMP_217_B":pandas.Series(timestamp_map["217"]["456"]),
        "TIMESTAMP_218_A":pandas.Series(timestamp_map["218"]["123"]),
        "TIMESTAMP_218_B":pandas.Series(timestamp_map["218"]["456"]),
        "TIMESTAMP_219_A":pandas.Series(timestamp_map["219"]["123"]),
        "TIMESTAMP_219_B":pandas.Series(timestamp_map["219"]["456"]),
        "DATETIME_217_A":pandas.Series(datetime_map["217"]["123"]),
        "DATETIME_217_B":pandas.Series(datetime_map["217"]["456"]),
        "DATETIME_218_A":pandas.Series(datetime_map["218"]["123"]),
        "DATETIME_218_B":pandas.Series(datetime_map["218"]["456"]),
        "DATETIME_219_A":pandas.Series(datetime_map["219"]["123"]),
        "DATETIME_219_B":pandas.Series(datetime_map["219"]["456"])
    })

    print(df)
    df.to_csv(csv_path)

if __name__ == "__main__":
    log_dir = ["log_217", "log_218", "log_219"]
    csv_name = "6hr_continuous_writing.csv"
    parse_log_to_csv(log_dir, csv_name)