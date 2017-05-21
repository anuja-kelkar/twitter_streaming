import pymysql
import pandas as pd
import json

with open('rds_config.json', 'r') as f:
    rds_config = json.load(f)

#db connection details
hostname = rds_config['host']
dbname = rds_config['dbname']
username = rds_config['username']
passwd = rds_config['password']
port = rds_config['port']


def fetch_results_from_db(sql_str):
    conn = pymysql.connect(host=hostname,
                             user=username,
                             password=passwd,
                             port=port,
                             db=dbname,
                             charset='utf8mb4')

    try:
        with conn.cursor() as cursor:
            cursor.execute(sql_str)
            columns = [col[0] for col in cursor.description]
            result = cursor.fetchall()

    finally:
        conn.close()
    return columns, result


def extract_data_into_df(sql_query):
    columns, result = fetch_results_from_db(sql_query.strip())
    res_df = pd.DataFrame(list(result), columns=columns)
    return res_df


def is_ascii(s):
    return all(ord(c) < 128 for c in s)


def flatten(lst):
    t = []
    for i in lst:
        if not isinstance(i, list):
            t.append(i)
        else:
            t.extend(flatten(i))
    return t


def is_json(json_str):
    try:
        json_object = json.loads(json_str)
    except ValueError:
        return False
    return True


def write_file(fname, res):
    with open(fname, "w") as json_f:
        json.dump(res, json_f)
    json_f.close()
