from elasticsearch import Elasticsearch
import argparse 
import json

def get_connection(uri):
    client = Elasticsearch(uri)
    print(client.info())
    return client

def write_to_file(data, file_name):
    with open(file_name, "w") as f:
        json.dump(data, f)

def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--uri",
        type=str,
        required=True,
        help="Elasticsearch uri http://localhost:1234"
    )

    parser.add_argument(
        "--query",
        type=str,
        required=True,
        help="Elasticsearch query as json string"
    )

    parser.add_argument(
        "--index",
        type=str,
        required=True,
        help="Elasticsearch index"
    )

    args = parser.parse_args()

    es_client = get_connection(args.uri)

    response = es_client.search(
        index=args.index,
        query=json.loads(args.query)
    )
    
    write_to_file(response, "filename.json")

if __name__ == "__main__":
    main()
