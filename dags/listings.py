import airflow
from airflow.operators.bash import BashOperator
from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator
import pendulum


@dag(
    start_date=airflow.utils.dates.days_ago(3),
    schedule="@hourly",
    catchup=False,
    template_searchpath="/tmp",
)
def listings():
    get_data = BashOperator(
        task_id="get_data",
        bash_command=(
            "curl -o /tmp/wikipageviews.gz "
            "https://dumps.wikimedia.org/other/pageviews/"
            "{{ execution_date.year }}/"
            "{{ execution_date.year }}-{{ '{:02}'.format(execution_date.month) }}/"
            "pageviews-{{ execution_date.year }}"
            "{{ '{:02}'.format(execution_date.month) }}"
            "{{ '{:02}'.format(execution_date.day) }}-"
            "{{ '{:02}'.format(execution_date.in_timezone('America/Sao_Paulo').hour) }}0000.gz"
        ),
    )

    extract_data = BashOperator(
        task_id="extract_data",
        bash_command=("gunzip -c /tmp/wikipageviews.gz > /tmp/wikipageviews"),
    )

    @task
    def fetch_pageviews(pagenames, execution_date: pendulum.DateTime, **kwargs):
        result = dict.fromkeys(pagenames, 0)

        with open("/tmp/wikipageviews", "r") as f:
            for line in f:
                domain_code, page_title, view_counts, *_ = line.split(" ")
                if domain_code == "en" and page_title in pagenames:
                    result[page_title] = view_counts

        with open("/tmp/postgres_query.sql", "w") as f:
            for pagename, pageviewcount in result.items():
                f.write(
                    (
                        "INSERT INTO pageview_counts (pagename, pageviewcount, datetime) "
                        f"VALUES ('{pagename}', {pageviewcount},'{ execution_date.in_timezone('America/Sao_Paulo')}');\n"
                    )
                )

        print(result)

        if result:
            return "done"
        return "fail"

    _fetch_pageviews = fetch_pageviews(
        pagenames={
            "Google",
            "Amazon",
            "Apple",
            "Microsoft",
            "Facebook",
        }
    )

    write_to_postgres = PostgresOperator(
        task_id="write_to_postgres",
        sql="postgres_query.sql",
        postgres_conn_id="postgres_5431",
    )

    get_data >> extract_data >> _fetch_pageviews >> write_to_postgres


listings()
