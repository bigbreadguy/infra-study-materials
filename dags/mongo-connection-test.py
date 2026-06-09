from datetime import datetime

# pyrefly: ignore [missing-import]
from airflow.sdk import DAG, task, Variable
# pyrefly: ignore [missing-import]
from airflow.providers.standard.operators.bash import BashOperator
# pyrefly: ignore [missing-import]
from airflow.providers.mongo.hooks.mongo import MongoHook

mongo_conn_id = Variable.get("mongo_conn_id", default="mongo-default-connection")

# A Dag represents a workflow, a collection of tasks
with DAG(
    dag_id="mongo-connection-test",
    start_date=datetime(2022, 1, 1),
    schedule=None
) as dag:
    # Tasks are represented as operators
    init = BashOperator(task_id="init", bash_command="echo 'Start connection test'")

    @task()
    def ping_mongo():
        with MongoHook(mongo_conn_id=mongo_conn_id) as hook:
            response = hook.get_conn().admin.command("ping")
        assert response["ok"] == 1.0
        print(response)

    # Set dependencies between tasks
    init >> ping_mongo()
