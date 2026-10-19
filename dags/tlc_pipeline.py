"""
TLC 出行数据管道 Airflow DAG

任务依赖：
  ods_yellow ─┐
              ├─ dwd_build ─ dws_build ─ ads_metrics ─ data_quality
  ods_green  ─┘

执行方式：每日增量（schedule_interval='0 6 * * *'，每天 06:00 跑前一天数据）
"""
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

BASE_DIR   = os.path.expanduser("~/tlc-spark-pipeline")
NOTEBOOKS  = f"{BASE_DIR}/notebooks"
CONDA_RUN  = "conda run -n tlc-spark"
NBCONVERT  = (
    f"{CONDA_RUN} jupyter nbconvert --to notebook --execute --inplace "
    "--ExecutePreprocessor.timeout=600"
)

default_args = {
    "owner":            "tlc-pipeline",
    "depends_on_past":  False,
    "start_date":       datetime(2024, 1, 1),
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="tlc_daily_pipeline",
    default_args=default_args,
    schedule_interval="0 6 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["tlc", "pyspark", "kafka"],
) as dag:

    start = EmptyOperator(task_id="start")

    # ODS 层：黄色 + 绿色并行入仓
    ods_yellow = BashOperator(
        task_id="ods_ingest_yellow",
        bash_command=(
            f"{NBCONVERT} {NOTEBOOKS}/01_ods_ingest.ipynb "
            "--ExecutePreprocessor.kernel_name=tlc-spark "
            f"&& echo 'ODS yellow done: {{{{ ds }}}}'"
        ),
    )

    ods_green = BashOperator(
        task_id="ods_ingest_green",
        bash_command=(
            f"{NBCONVERT} {NOTEBOOKS}/01_ods_ingest.ipynb "
            "--ExecutePreprocessor.kernel_name=tlc-spark "
            f"&& echo 'ODS green done: {{{{ ds }}}}'"
        ),
    )

    # DWD 层：两路 ODS 合并后统一清洗
    dwd_build = BashOperator(
        task_id="dwd_build",
        bash_command=(
            f"{NBCONVERT} {NOTEBOOKS}/02_dwd_build.ipynb "
            "--ExecutePreprocessor.kernel_name=tlc-spark"
        ),
    )

    # DWS 层：日粒度聚合（含加盐去倾斜）
    dws_build = BashOperator(
        task_id="dws_build",
        bash_command=(
            f"{NBCONVERT} {NOTEBOOKS}/03_dws_build.ipynb "
            "--ExecutePreprocessor.kernel_name=tlc-spark"
        ),
    )

    # ADS 层：KPI 指标
    ads_metrics = BashOperator(
        task_id="ads_metrics",
        bash_command=(
            f"{NBCONVERT} {NOTEBOOKS}/04_ads_metrics.ipynb "
            "--ExecutePreprocessor.kernel_name=tlc-spark"
        ),
    )

    # 数据质量检查：作为调度节点，FAIL 则阻断后续（通过 task 失败实现）
    data_quality = BashOperator(
        task_id="data_quality_check",
        bash_command=(
            f"{NBCONVERT} {NOTEBOOKS}/05_data_quality.ipynb "
            "--ExecutePreprocessor.kernel_name=tlc-spark"
        ),
    )

    end = EmptyOperator(task_id="end")

    # 依赖关系
    start >> [ods_yellow, ods_green] >> dwd_build >> dws_build >> ads_metrics >> data_quality >> end
