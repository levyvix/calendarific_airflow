import datetime
import json
import logging

import requests
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, get_current_context
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import (
    LocalFilesystemToS3Operator,
)

# trigger dag
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from pandas import json_normalize

S3_CONN_ID = "aws_default"
BUCKET_NAME = "airflow-levy"


@dag(
    default_args={"owner": "levy"},
    schedule="@daily",
    start_date=datetime.datetime(2021, 1, 1),
    tags=["holidays"],
    catchup=False,
)
def dag_holiday_generator(
    url_base: str, endpoint: str, api_key: str, country: str, year: list[str]
):
    @task(retries=3)
    def get_json_response() -> bool:
        context = get_current_context()

        # parse all arguments into string
        url_base_str = url_base.resolve(context)
        endpoint_str = endpoint.resolve(context)
        api_key_str = api_key.resolve(context)
        country_str = country.resolve(context)
        year_list = year.resolve(context)

        logging.info(f"year_list: {year_list}")

        if len(year_list) > 0:
            for year_str in year_list:
                response = requests.get(
                    url_base_str + endpoint_str,
                    params={
                        "api_key": api_key_str,
                        "country": country_str,
                        "year": int(year_str),
                    },
                )
                response.raise_for_status()

                # Save JSON data to a file
                logging.info(f'writing data to /tmp/holidays_{year_str}.json')

                with open(f"/tmp/holidays_{year_str}.json", "w") as json_file:
                    json.dump(response.json()["response"]["holidays"], json_file)

        return True

    @task()
    def write_dataframe() -> str:
        context = get_current_context()

        # parse all arguments into string
        year_list = year.resolve(context)

        # Convert the generator to a DataFrame
        for i, year_str in enumerate(year_list):
            with open(f"/tmp/holidays_{year_str}.json") as json_file:
                json_data = json.load(json_file)
            
            dataframe = json_normalize(json_data)

            if i == 0:
                dataframe.to_csv(
                    "/tmp/holidays.csv", index=False, mode="w", header=True
                )
            else:
                dataframe.to_csv(
                    "/tmp/holidays.csv",
                    index=False,
                    mode="a",
                    header=False,
                )

        return "done"

    email_operator = EmailOperator(
        task_id="send_email",
        to="levy.vix@gmail.com",
        subject="Holidays API failed",
        html_content="<p>Something went wrong with the Holidays API</p>",
    )

    def _check_status_code(response_code):
        return "write_dataframe" if response_code else "send_email"

    json_response = get_json_response()

    branch_operator = BranchPythonOperator(
        task_id="is_status_code_200",
        python_callable=_check_status_code,
        op_args=[json_response],
    )

    create_bucket = S3CreateBucketOperator(
        task_id="create_bucket",
        bucket_name=BUCKET_NAME,
        region_name="us-east-1",
        aws_conn_id=S3_CONN_ID,
    )

    to_s3 = LocalFilesystemToS3Operator(
        task_id="to_s3",
        filename="/tmp/holidays.csv",
        dest_key=f"s3://{BUCKET_NAME}" + "/holidays/holidays.csv",
        replace=True,
        aws_conn_id=S3_CONN_ID,
    )

    remove_local_files = BashOperator(
        task_id="remove_local_files",
        # delete files from /tmp
        bash_command="rm -rf /tmp/holidays.csv /tmp/holidays_*.json",
    )

    empty = EmptyOperator(task_id="empty")

    write_dataframe_done = write_dataframe()

    # write_dataframe()
    # done = write_dataframe(holiday_generator)
    json_response >> branch_operator >> [write_dataframe_done, email_operator]

    write_dataframe_done >> create_bucket >> to_s3 >> remove_local_files
    email_operator >> empty

    run_glue_crawler = TriggerDagRunOperator(
        task_id="run_glue_crawler",
        trigger_dag_id="glue_operations",
        conf={"year": year},
    )

    remove_local_files >> run_glue_crawler


dag_holiday_generator(
    url_base="https://calendarific.com/api/v2",
    endpoint="/holidays",
    api_key="OhB6EkvGcnYIOwBW25PrUH1u9WMyA8DK",
    country="BR",
    year=['2024', '2023'],
)
