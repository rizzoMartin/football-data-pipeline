# Football pipeline DAG bronze - check - silver - gold
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator, ShortCircuitOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs


def get_client():
    return WorkspaceClient(
        host=Variable.get("DATABRICKS_HOST"),
        token=Variable.get("DATABRICKS_TOKEN")
    )


def run_notebook(notebook_path, read_output=False):
    client = get_client()

    run = client.jobs.submit(
        run_name=f"airflow_{notebook_path.split('/')[-1]}",
        tasks=[
            jobs.SubmitTask(
                task_key="notebook_task",
                notebook_task=jobs.NotebookTask(
                    notebook_path=notebook_path,
                    source=jobs.Source.WORKSPACE
                ),
                environment_key="default"
            )
        ],
        environments=[
            jobs.JobEnvironment(
                environment_key="default",
                spec=jobs.compute.Environment(client="2")
            )
        ]
    ).result()

    if read_output:
        output = client.jobs.get_run_output(run_id=run.tasks[0].run_id)
        result = output.notebook_output.result if output.notebook_output else None
        print(f"Resultado notebook: {result}")
        return result


def run_bronze():
    return run_notebook(Variable.get("DATABRICKS_NOTEBOOK_BRONZE"), read_output=True)


def check_matches(**context):
    result = context["ti"].xcom_pull(task_ids="ingest_bronze")
    print(f"Resultado check_matches: {result}")
    return result != "NO_MATCHES"


def run_silver():
    run_notebook(Variable.get("DATABRICKS_NOTEBOOK_SILVER"))


def run_gold():
    run_notebook(Variable.get("DATABRICKS_NOTEBOOK_GOLD"))


with DAG(
    dag_id="football_pipeline",
    description="Pipeline diario de datos de las 5 grandes ligas europeas",
    start_date=datetime(2024, 1, 1),
    schedule="40 23 * * *",
    catchup=False
) as dag:

    bronze = PythonOperator(
        task_id="ingest_bronze",
        python_callable=run_bronze,
        retries=2,
        retry_delay=timedelta(minutes=5)
    )

    check = ShortCircuitOperator(
        task_id="check_matches",
        python_callable=check_matches,
        retries=2,
        retry_delay=timedelta(minutes=5)
    )

    silver = PythonOperator(
        task_id="transform_silver",
        python_callable=run_silver,
        retries=2,
        retry_delay=timedelta(minutes=5)
    )

    gold = PythonOperator(
        task_id="compute_gold",
        python_callable=run_gold,
        retries=2,
        retry_delay=timedelta(minutes=5)
    )

    bronze >> check >> silver >> gold