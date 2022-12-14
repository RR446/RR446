import pymongo
import argparse 
import json
import os

def get_connection(uri):
    client = pymongo.MongoClient(uri)
    print(client.info())
    return client.cursor()


def write_to_jsonl_file(data, file_name):
    with open(file_name, "w") as f:
        for item in data:
            json.dump(item, f)
            f.write("\n")


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--uri",
        type=str,
        required=True,
        help="mongodb uri"
    )

    parser.add_argument(
        "--collection",
        type=str,
        required=True,
        help="mongodb collection"
    )

    parser.add_argument(
        "--database",
        type=str,
        required=True,
        help="mongodb database"
    )


    args = parser.parse_args()

    client = get_connection(args.uri)

    response = client[args.collection][args.database]
    
    data = [record for record in response.find()]

    write_to_jsonl_file(data, "filename.jsonl")

if __name__ == "__main__":
    main()