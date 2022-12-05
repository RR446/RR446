import argparse 
import apache_beam as beam

from apache_beam.beam.io.mongodbio import ReadFromMongoDB
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
        "--uri",
        type=str,
        required=True,
        help="Mongo DB uri"
    )
    parser.add_argument(
        "--database",
        type=str,
        required=True,
        help="MongoDB Database name "
    )
    parser.add_argument(
        "--collection",
        type=str,
        required=True,
        help="MongoDB connection"
    )

    args = parser.parse_args()

    beam_options = PipelineOptions(
        runner=args.runner,
        project=args.project,
        region=args.region,
        job_name="dataflow-mongodb",
        temp_location=f"gs://{args.bucket}/temp",
    )

    with beam.Pipeline(options=beam_options) as pipeline:
        read_data = ( pipeline
                    | ReadFromMongoDB(uri=args.uri,
                                        db=args.database,
                                        coll=args.collection)
                    | WriteToText(f"gs://{args.bucket}/data/filename.json")
                    )


if __name__ == "__main__":
    run()
