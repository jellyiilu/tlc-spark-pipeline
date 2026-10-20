"""
TLC 出行数据管道 Airflow DAG

任务依赖：
  ods_yellow ─┐
  ods_green  ─┼─ dq_after_ods ─ dwd_build ─ dq_after_dwd ─ dws_build ─ dq_after_dws ─ ads_metrics
  ods_fhv    ─┘

DQ 检查作为门控节点：FAIL 则抛出异常，下游任务不执行，脏数据不会污染后续层。
  dq_after_ods : 行数非空（block）+ 同周同日波动检测（warn），防止空分区流入 DWD
  dq_after_dwd : 完整性/范围/时序/行数守恒/参照完整性（Rules 1-16）
  dq_after_dws : 聚合守恒（Rule 17）

增量策略：Airflow 通过环境变量 TARGET_DATE={{ ds }} 传入目标日期。
catchup=True + end_date=2024-03-31：注册 DAG 后 Airflow 会自动回填 Q1 2024 全部 91 天。
max_active_runs=1 确保回填串行执行，ODS → DWD → DWS → ADS 严格顺序，避免并发写冲突。

注意：三个 ODS 任务并行运行，但各自写不同 source_type 分区，不存在写冲突。
每个任务使用独立输出路径（/tmp/），避免 --inplace 互相覆盖同一个 .ipynb 文件。
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
    f"{CONDA_RUN} jupyter nbconvert --to notebook --execute "
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
    catchup=True,                        # 注册后自动回填 start_date → end_date 全部 91 天
    end_date=datetime(2024, 3, 31, 23, 59),  # 包含 3/31 06:00 那次运行（end_date 不含同秒，设 23:59 确保 Q1 最后一天被调度）
    max_active_runs=1,                   # 回填串行执行，防止并发写冲突
    tags=["tlc", "pyspark", "kafka"],
) as dag:

    # {{ ds }} = logical_date，即本次运行负责的日期（Q1 回填时为 2024-01-01 ~ 2024-03-31）
    TARGET_DATE_ENV = {"TARGET_DATE": "{{ ds }}"}

    start = EmptyOperator(task_id="start")

    # ODS 层：三路数据源并行入仓，只处理 TARGET_DATE 对应文件。
    # 使用 --output /tmp/ 而非 --inplace，避免三个任务同时写同一个 .ipynb 文件互相覆盖。
    # append_env=True：将 TARGET_DATE/SOURCE 合并到进程环境，保留 PATH/JAVA_HOME 等系统变量。
    ods_yellow = BashOperator(
        task_id="ods_ingest_yellow",
        bash_command=(
            f"{NBCONVERT} {NOTEBOOKS}/01_ods_ingest.ipynb "
            "--ExecutePreprocessor.kernel_name=tlc-spark "
            f"--output /tmp/01_ods_yellow_{{{{ ds_nodash }}}}.ipynb "
            f"&& echo 'ODS yellow done: {{{{ ds }}}}'"
        ),
        env={**TARGET_DATE_ENV, "SOURCE": "yellow"},
        append_env=True,
    )

    ods_green = BashOperator(
        task_id="ods_ingest_green",
        bash_command=(
            f"{NBCONVERT} {NOTEBOOKS}/01_ods_ingest.ipynb "
            "--ExecutePreprocessor.kernel_name=tlc-spark "
            f"--output /tmp/01_ods_green_{{{{ ds_nodash }}}}.ipynb "
            f"&& echo 'ODS green done: {{{{ ds }}}}'"
        ),
        env={**TARGET_DATE_ENV, "SOURCE": "green"},
        append_env=True,
    )

    ods_fhv = BashOperator(
        task_id="ods_ingest_fhv",
        bash_command=(
            f"{NBCONVERT} {NOTEBOOKS}/01_ods_ingest.ipynb "
            "--ExecutePreprocessor.kernel_name=tlc-spark "
            f"--output /tmp/01_ods_fhv_{{{{ ds_nodash }}}}.ipynb "
            f"&& echo 'ODS fhv done: {{{{ ds }}}}'"
        ),
        env={**TARGET_DATE_ENV, "SOURCE": "fhv_hv"},
        append_env=True,
    )

    # DQ 门控 — ODS 之后（行数非空 block + 同周同日波动 warn，FAIL 阻断 DWD）
    dq_after_ods = BashOperator(
        task_id="dq_after_ods",
        bash_command=(
            f"{NBCONVERT} {NOTEBOOKS}/02_dq_after_ods.ipynb "
            "--ExecutePreprocessor.kernel_name=tlc-spark "
            f"--output /tmp/02_dq_ods_{{{{ ds_nodash }}}}.ipynb"
        ),
        env=TARGET_DATE_ENV,
        append_env=True,
        retries=0,
    )

    # DWD 层：三路 ODS 合并后统一清洗，只处理 TARGET_DATE 分区
    dwd_build = BashOperator(
        task_id="dwd_build",
        bash_command=(
            f"{NBCONVERT} {NOTEBOOKS}/03_dwd_build.ipynb "
            "--ExecutePreprocessor.kernel_name=tlc-spark "
            f"--output /tmp/03_dwd_{{{{ ds_nodash }}}}.ipynb"
        ),
        env=TARGET_DATE_ENV,
        append_env=True,
    )

    # DQ 门控 — DWD 之后（Rules 1-16，blocking FAIL 阻断 DWS）
    dq_after_dwd = BashOperator(
        task_id="dq_after_dwd",
        bash_command=(
            f"{NBCONVERT} {NOTEBOOKS}/04_dq_after_dwd.ipynb "
            "--ExecutePreprocessor.kernel_name=tlc-spark "
            f"--output /tmp/04_dq_dwd_{{{{ ds_nodash }}}}.ipynb"
        ),
        env=TARGET_DATE_ENV,
        append_env=True,
        retries=0,
    )

    # DWS 层：日粒度聚合（含加盐两阶段），只处理 TARGET_DATE
    dws_build = BashOperator(
        task_id="dws_build",
        bash_command=(
            f"{NBCONVERT} {NOTEBOOKS}/05_dws_build.ipynb "
            "--ExecutePreprocessor.kernel_name=tlc-spark "
            f"--output /tmp/05_dws_{{{{ ds_nodash }}}}.ipynb"
        ),
        env=TARGET_DATE_ENV,
        append_env=True,
    )

    # DQ 门控 — DWS 之后（Rule 17，FAIL 阻断 ADS）
    dq_after_dws = BashOperator(
        task_id="dq_after_dws",
        bash_command=(
            f"{NBCONVERT} {NOTEBOOKS}/06_dq_after_dws.ipynb "
            "--ExecutePreprocessor.kernel_name=tlc-spark "
            f"--output /tmp/06_dq_dws_{{{{ ds_nodash }}}}.ipynb"
        ),
        env=TARGET_DATE_ENV,
        append_env=True,
        retries=0,
    )

    # ADS 层：KPI 指标（动态分区覆盖，幂等），只处理 TARGET_DATE
    ads_metrics = BashOperator(
        task_id="ads_metrics",
        bash_command=(
            f"{NBCONVERT} {NOTEBOOKS}/07_ads_metrics.ipynb "
            "--ExecutePreprocessor.kernel_name=tlc-spark "
            f"--output /tmp/07_ads_{{{{ ds_nodash }}}}.ipynb"
        ),
        env=TARGET_DATE_ENV,
        append_env=True,
    )

    end = EmptyOperator(task_id="end")

    # 依赖关系：DQ 作为门控节点，每层之后立即检查
    start >> [ods_yellow, ods_green, ods_fhv] >> dq_after_ods >> dwd_build >> dq_after_dwd >> dws_build >> dq_after_dws >> ads_metrics >> end
