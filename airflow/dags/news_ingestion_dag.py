from __future__ import annotations

import sys
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

sys.path.append("/opt/airflow/project")

from services.common.config import load_clubs  # noqa: E402
from services.common.settings import Settings  # noqa: E402

settings = Settings()
clubs, _ = load_clubs(settings)

TZ = pendulum.timezone("America/Fortaleza")

DEFAULT_ARGS = {
    "owner": "torcida-news-rag",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}

def build_dag(dag_id: str, schedule: str, description: str) -> DAG:
    with DAG(
        dag_id=dag_id,
        description=description,
        schedule=schedule,
        start_date=pendulum.datetime(2025, 1, 1, tz=TZ),
        catchup=False,
        default_args=DEFAULT_ARGS,
        tags=["rss", "news", "torcida"],
    ) as dag:
        start = EmptyOperator(task_id="start")
        end = EmptyOperator(task_id="end")

        with TaskGroup(group_id="ingest_clubs") as ingest_group:
            for club in clubs:
                task_safe = club.id.replace("-", "_")
                BashOperator(
                    task_id=f"ingest_{task_safe}",
                    bash_command=(
                        "python -m services.ingestion.main "
                        f"--club {club.id} --mode ingest"
                    ),
                    env={"PYTHONPATH": "/opt/airflow/project"},
                )

        with TaskGroup(group_id="select_daily_picks") as select_group:
            for club in clubs:
                task_safe = club.id.replace("-", "_")
                BashOperator(
                    task_id=f"select_{task_safe}",
                    bash_command=(
                        "python -m services.ingestion.main "
                        f"--club {club.id} --mode select --date {{{{ ds }}}}"
                    ),
                    env={"PYTHONPATH": "/opt/airflow/project"},
                )

        start >> ingest_group >> select_group >> end

    return dag


dag = build_dag(
    dag_id="torcida_news_ingestion",
    schedule="0 6,14,18,20 * * *",
    description="RSS ingestion (06:00, 14:00, 18:00, 20:00)",
)

dag_0030 = build_dag(
    dag_id="torcida_news_ingestion_0030",
    schedule="30 0 * * *",
    description="RSS ingestion (00:30)",
)
