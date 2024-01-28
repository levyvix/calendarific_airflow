import datetime
import logging
import requests
import json
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.python import get_current_context, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.email import EmailOperator
from pandas import json_normalize, DataFrame, concat


@dag(
    default_args={"owner": "levy"},
    schedule="@daily",
    start_date=datetime.datetime(2021, 1, 1),
    tags=["holidays"],
    catchup=False,
)
def dag_holiday_generataor(
    url_base: str, endpoint: str, api_key: str, country: str, year: str
):
    @task()
    def get_json_response():
        context = get_current_context()

        date = context.get("ds_nodash")

        logging.info(date)

        # parse all arguments into string
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

        # Save JSON data to a file
        with open("/tmp/holidays.json", "w") as json_file:
            json.dump(response.json()["response"]["holidays"], json_file)

        return True if response.json()["meta"]["code"] == 200 else False

    @task()
    def write_dataframe():
        context = get_current_context()
        year_str = year.resolve(context)

        def get_holidays():
            with open("/tmp/holidays.json", "r") as json_file:
                holidays = json.load(json_file)
                for holiday in holidays:
                    yield holiday

        # Convert the generator to a DataFrame
        for i, holiday in enumerate(get_holidays()):
            dataframe = json_normalize(holiday)

            if i == 0:
                dataframe.to_csv(
                    f"/tmp/holidays_{year_str}.csv", index=False, mode="w", header=True
                )
            else:
                dataframe.to_csv(
                    f"/tmp/holidays_{year_str}.csv", index=False, mode="a", header=False
                )

        # dataframes.to_csv(f"/tmp/holidays_{year_str}.csv", index=False)
        return "done"

    bash_operator = BashOperator(
        task_id="date",
        bash_command="echo  {{ ds }}",
    )

    json_response = get_json_response()

    email_operator = EmailOperator(
        task_id="send_email",
        to="levy.vix@gmail.com",
        subject="Holidays API failed",
        html_content="<p>Something went wrong with the Holidays API</p>",
    )

    def _check_status_code(response_code):
        return "write_dataframe" if response_code else "send_email"

    branch_operator = BranchPythonOperator(
        task_id="is_status_code_200",
        python_callable=_check_status_code,
        op_args=[json_response],
    )

    empty = EmptyOperator(task_id="empty")

    done = write_dataframe()

    # write_dataframe()
    # done = write_dataframe(holiday_generator)
    json_response >> branch_operator >> [done, email_operator]

    email_operator >> empty
    done >> bash_operator
    


dag_holiday_generataor(
    url_base="https://calendarific.com/api/v2",
    endpoint="/holidays",
    api_key="OhB6EkvGcnYIOwBW25PrUH1u9WMyA8DK",
    country="BR",
    year=2024,
)
