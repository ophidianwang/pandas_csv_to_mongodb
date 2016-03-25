import re
import pandas

def parse_log_to_csv(log_path, csv_path):
    before_count_list = []
    before_time_list = []
    spend_time_list = []
    after_count_list = []
    after_time_list = []
    with open(log_path, "r") as log_file_1:
        for line in log_file_1:
            if line[:4] == "work":
                continue
            if line[0] == "[":
                continue
            # print(line)

            # before message
            match = re.match(r"before insert\: (?P<count>.*)\, \@ (?P<datetime>.*)", line.strip())
            if match:
                # print(match.group("count"))
                # print(match.group("datetime"))
                before_count_list.append(match.group("count"))
                before_time_list.append(match.group("datetime"))
                continue
            # spend message
            match = re.match(r"spend (?P<seconds>.*) seconds.*", line.strip())
            if match:
                # print(match.group("seconds"))
                spend_time_list.append(match.group("seconds"))
                continue
            # after message
            match = re.match(r"after insert\: (?P<count>.*)\, \@ (?P<datetime>.*)", line.strip())
            if match:
                # print(match.group("count"))
                # print(match.group("datetime"))
                after_count_list.append(match.group("count"))
                after_time_list.append(match.group("datetime"))
                continue

    df = pandas.DataFrame({
        "RECORD_BEFORE":pandas.Series(before_count_list),
        "TIME_BEFORE":pandas.Series(before_time_list),
        "RECORD_AFTER":pandas.Series(after_count_list),
        "TIME_AFTER": pandas.Series(after_time_list),
        "SPEND_TIME": pandas.Series(spend_time_list)
    })

    print(df)
    df.to_csv(csv_path)

if __name__ == "__main__":
    log_files = ["check_speed_multi_1.log",
                 "check_speed_multi_2.log",
                 "check_speed_multi_3.log"
                 ]

    for filename in log_files:
        tmp = filename.split(".")
        csv_name = tmp[0] + ".csv"
        parse_log_to_csv(filename, csv_name)