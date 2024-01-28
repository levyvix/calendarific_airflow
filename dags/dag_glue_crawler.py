import datetime

import boto3
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator

BUCKET_NAME = "airflow-levy"


@dag(
    default_args={"owner": "levy"},
    schedule="@daily",
    start_date=datetime.datetime(2021, 1, 1),
    tags=["holidays"],
    catchup=False,
)
def glue_operations():
    @task
    def create_classifier():
        aws_hook = AwsBaseHook(aws_conn_id="aws_default")

        aws_conn = aws_hook.get_connection(conn_id="aws_default")

        glue_client = boto3.client(
            "glue",
            region_name="us-east-1",
            aws_access_key_id=aws_conn.login,
            aws_secret_access_key=aws_conn.password,
        )
        try:
            glue_client.create_classifier(
                CsvClassifier={
                    "Name": "calendarific_csv",
                    "Delimiter": ",",
                    "QuoteSymbol": '"',
                    "ContainsHeader": "PRESENT",
                    "Header": [
                        "name",
                        "description",
                        "country",
                        "date",
                        "type",
                        "primary_type",
                        "canonical_url",
                        "urlid",
                        "locations",
                        "states",
                    ],
                }
            )
        except glue_client.exceptions.AlreadyExistsException:
            return 'already exists'

        return 'created'

    glue_crawler_config = {
        "Name": "airflow-levy-crawler",
        "Role": "airflow-levi-role",
        "DatabaseName": "holidays",
        "Targets": {
            "S3Targets": [
                {
                    "Path": f"s3://{BUCKET_NAME}/holidays",
                }
            ]
        },
        'Classifiers': ['calendarific_csv'],
    }

    crawl_s3 = GlueCrawlerOperator(
        task_id="crawl_s3",
        config=glue_crawler_config,
        region_name="us-east-1",
    )

    # submit_glue_job = GlueJobOperator(
    #     task_id="submit_glue_job",
    #     job_name=glue_job_name,
    #     script_location=f"s3://{bucket_name}/etl_script.py",
    #     s3_bucket=bucket_name,
    #     iam_role_name=role_name,
    #     create_job_kwargs={"GlueVersion": "3.0", "NumberOfWorkers": 2, "WorkerType": "G.1X"},
    # )

    create_classifier_task = create_classifier()

    create_classifier_task >> crawl_s3


glue_operations()
