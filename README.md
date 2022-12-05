mport argparse 
import apache_beam as beam
import os

from beam_mysql.connector.io import ReadFromMySQL
from apache_beam.beam.io.textio import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions

parser = argparse.ArgumentParser()

def run():
    parser.add_argument(
        "--runner",
        type=str,
        required=True,
        help="DataflowRunner or DirectRunner"
    )
    parser.add_argument(
        "--project",
        type=str,
        required=True,
        help="GCP Project id"
    )
    parser.add_argument(
        "--region",
        type=str,
        required=True,
        help="GCP Project region"
    )
    parser.add_argument(
        "--bucket",
        type=str,
        required=True,
        help="GCP Bucket name"
    )
    parser.add_argument(
        "--host",
        type=str,
        required=True,
        help="MySql hostname"
    )
    parser.add_argument(
        "--database",
        type=str,
        required=True,
        help="MySQL Database name "
    )
    parser.add_argument(
        "--query",
        type=str,
        required=True,
        help="MySQL Query"
    )

    args = parser.parse_args()

    beam_options = PipelineOptions(
        runner=args.runner,
        project=args.project,
        region=args.region,
        job_name="dataflow-mysql",
        temp_location=f"gs://{args.bucket}/temp",
    )

    with beam.Pipeline(options=beam_options) as pipeline:
        read_data = ( pipeline
                    | ReadFromMySQL(
                        query=args.query,
                        host=args.host,
                        database=args.database,
                        user=os.getenv("username"),
                        password=os.getenv("password"),
                        port=3306,
                    )
                    | WriteToText(f"gs://{args.bucket}/data/filename.csv")
                    )


if __name__ == "__main__":
    run()
