import mysql.connector as sql
import pandas as pd
import argparse 
import json
import os

def get_connection(host, port, database, username):
    client = sql.connect(
        host = host,
        port = port,
        database = database,
        user = username,
        password = os.getenv("mysql_password")
    )
    print(client.info())
    return client.cursor()


def write_to_jsonl_file(data, file_name):
    with open(file_name, "w") as f:
        for item in data:
            json.dump(item, f)
            f.write("\n")

def read_to_dataframe(data):
    df = pd.DataFrame(data)
    return df

def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--host",
        type=str,
        required=True,
        help="mysql hostname"
    )

    parser.add_argument(
        "--port",
        type=str,
        required=True,
        help="mysql port"
    )

    parser.add_argument(
        "--database",
        type=str,
        required=True,
        help="mysql database"
    )

    parser.add_argument(
        "--username",
        type=str,
        required=True,
        help="mysql username"
    )
    
    parser.add_argument(
        "--query",
        type=str,
        required=True,
        help="mysql query as json string"
    )

    args = parser.parse_args()

    mysql_cursor = get_connection(args.host, args.port, args.database, args.username)

    response = mysql_cursor.execute(
        query=args.query
    )
    
    data_frame = read_to_dataframe(response.fetchall())

    write_to_jsonl_file(data_frame.to_json(), "filename.jsonl")

if __name__ == "__main__":
    main()