import datetime
import logging

import requests
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.python import get_current_context
from pandas import json_normalize


@dag(
    default_args={"owner": "levy"},
    schedule="@daily",
    start_date=datetime.datetime(2021, 1, 1),
    tags=["holidays"],
    catchup=False,
)
def dag_holiday(url_base: str, endpoint: str, api_key: str, country: str, year: str):

    @task(multiple_outputs=True)
    def get_json_response():
        context = get_current_context()

        date = context.get("ds_nodash")

        logging.info(date)

        # parse all arguments intro string
        url_base_str = url_base.resolve(context)
        endpoint_str = endpoint.resolve(context)
        api_key_str = api_key.resolve(context)
        country_str = country.resolve(context)
        year_str = year.resolve(context)

        response = requests.get(
            url_base_str + endpoint_str,
            params={"api_key": api_key_str, "country": country_str, "year": year_str},
        )
        response.raise_for_status()
        logging.info(response.json())

        return {
            "holidays": response.json()["response"]["holidays"],
            "status_code": response.json()["meta"]["code"],
        }

    @task()
    def normalize_json(json_response: dict):
        return json_normalize(json_response["holidays"])

    @task()
    def write_dataframe(df):
        context = get_current_context()
        year_str = year.resolve(context)

        df.to_csv(f"/tmp/holidays_{year_str}.csv", index=False)
        return "done"

    bash_operator = BashOperator(
        task_id="print_working_directory",
        bash_command="echo  {{ ds }}",
    )

    json_response = get_json_response()
    df = normalize_json(json_response)
    done = write_dataframe(df)

    json_response >> df >> done >> bash_operator


dag_holiday(
    url_base="https://calendarific.com/api/v2",
    endpoint="/holidays",
    api_key="OhB6EkvGcnYIOwBW25PrUH1u9WMyA8DK",
    country="BR",
    year=2024,
)
