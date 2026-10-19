"""
TLC 出行数据管道 Airflow DAG

任务依赖：
  ods_yellow ─┐
  ods_green  ─┼─ dq_after_ods ─ dwd_build ─ dq_after_dwd ─ dws_build ─ dq_after_dws ─ ads_metrics
  ods_fhv    ─┘

DQ 检查作为门控节点：FAIL 则抛出异常，下游任务不执行，脏数据不会污染后续层。
  dq_after_ods : 行数非空 + 月度波动检测，防止空分区流入 DWD
  dq_after_dwd : 完整性/范围/时序/行数守恒/参照完整性（Rules 1-12）
  dq_after_dws : 聚合守恒（Rule 13）

增量策略：Airflow 通过环境变量 TARGET_DATE={{ ds }} 传入目标日期。
{{ ds }} 在 Airflow 2 中表示本次运行负责的逻辑日期（即前一天），
每次只处理该日期数据，动态分区覆盖只覆盖对应日分区，不影响历史数据。
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

    # {{ ds }} 在 Airflow 2 中即为本次运行的逻辑日期（前一天）
    TARGET_DATE_ENV = {"TARGET_DATE": "{{ ds }}"}

    start = EmptyOperator(task_id="start")

    # ODS 层：三路数据源并行入仓，只处理 TARGET_DATE 对应文件
    ods_yellow = BashOperator(
        task_id="ods_ingest_yellow",
        bash_command=(
            f"{NBCONVERT} {NOTEBOOKS}/01_ods_ingest.ipynb "
            "--ExecutePreprocessor.kernel_name=tlc-spark "
            f"&& echo 'ODS yellow done: {{{{ ds }}}}'"
        ),
        env=TARGET_DATE_ENV,
    )

    ods_green = BashOperator(
        task_id="ods_ingest_green",
        bash_command=(
            f"{NBCONVERT} {NOTEBOOKS}/01_ods_ingest.ipynb "
            "--ExecutePreprocessor.kernel_name=tlc-spark "
            f"&& echo 'ODS green done: {{{{ ds }}}}'"
        ),
        env=TARGET_DATE_ENV,
    )

    ods_fhv = BashOperator(
        task_id="ods_ingest_fhv",
        bash_command=(
            f"{NBCONVERT} {NOTEBOOKS}/01_ods_ingest.ipynb "
            "--ExecutePreprocessor.kernel_name=tlc-spark "
            f"&& echo 'ODS fhv done: {{{{ ds }}}}'"
        ),
        env=TARGET_DATE_ENV,
    )

    # DQ 门控 — ODS 之后（行数非空 + 月度波动，FAIL 阻断 DWD）
    dq_after_ods = BashOperator(
        task_id="dq_after_ods",
        bash_command=(
            f"{NBCONVERT} {NOTEBOOKS}/05c_dq_after_ods.ipynb "
            "--ExecutePreprocessor.kernel_name=tlc-spark"
        ),
        env=TARGET_DATE_ENV,
    )

    # DWD 层：三路 ODS 合并后统一清洗，只处理 TARGET_DATE 分区
    dwd_build = BashOperator(
        task_id="dwd_build",
        bash_command=(
            f"{NBCONVERT} {NOTEBOOKS}/02_dwd_build.ipynb "
            "--ExecutePreprocessor.kernel_name=tlc-spark"
        ),
        env=TARGET_DATE_ENV,
    )

    # DQ 门控 — DWD 之后（Rules 1-12，FAIL 阻断 DWS）
    dq_after_dwd = BashOperator(
        task_id="dq_after_dwd",
        bash_command=(
            f"{NBCONVERT} {NOTEBOOKS}/05a_dq_after_dwd.ipynb "
            "--ExecutePreprocessor.kernel_name=tlc-spark"
        ),
        env=TARGET_DATE_ENV,
    )

    # DWS 层：日粒度聚合（含加盐去倾斜），只处理 TARGET_DATE
    dws_build = BashOperator(
        task_id="dws_build",
        bash_command=(
            f"{NBCONVERT} {NOTEBOOKS}/03_dws_build.ipynb "
            "--ExecutePreprocessor.kernel_name=tlc-spark"
        ),
        env=TARGET_DATE_ENV,
    )

    # DQ 门控 — DWS 之后（Rule 13，FAIL 阻断 ADS）
    dq_after_dws = BashOperator(
        task_id="dq_after_dws",
        bash_command=(
            f"{NBCONVERT} {NOTEBOOKS}/05b_dq_after_dws.ipynb "
            "--ExecutePreprocessor.kernel_name=tlc-spark"
        ),
        env=TARGET_DATE_ENV,
    )

    # ADS 层：KPI 指标，只处理 TARGET_DATE
    ads_metrics = BashOperator(
        task_id="ads_metrics",
        bash_command=(
            f"{NBCONVERT} {NOTEBOOKS}/04_ads_metrics.ipynb "
            "--ExecutePreprocessor.kernel_name=tlc-spark"
        ),
        env=TARGET_DATE_ENV,
    )

    end = EmptyOperator(task_id="end")

    # 依赖关系：DQ 作为门控节点，每层之后立即检查
    start >> [ods_yellow, ods_green, ods_fhv] >> dq_after_ods >> dwd_build >> dq_after_dwd >> dws_build >> dq_after_dws >> ads_metrics >> end
