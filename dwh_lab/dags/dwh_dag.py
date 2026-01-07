from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
import subprocess
import os
import sys
import logging

def ingest_batch_sources_full_load(entity_path):
    import logging
    log = logging.getLogger("airflow.task")

    script_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'dwh', 'ingest_batch_sources_full_load.py'))

    args = ["--entity_path", entity_path]
    process = subprocess.Popen(
        [sys.executable, "-u", script_path] + args,  # -u forces unbuffered output
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1
    )

    for line in process.stdout:
        log.info(line.strip())

    process.wait()
    if process.returncode != 0:
        raise subprocess.CalledProcessError(process.returncode, process.args)
    
def ingest_click_house_full_load(entity_name):
    import logging
    log = logging.getLogger("airflow.task")

    script_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'dwh', 'ingest_click_house_full_load.py'))

    args = ["--entity_name", entity_name]
    process = subprocess.Popen(
        [sys.executable, "-u", script_path] + args,  # -u forces unbuffered output
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1
    )

    for line in process.stdout:
        log.info(line.strip())

    process.wait()
    if process.returncode != 0:
        raise subprocess.CalledProcessError(process.returncode, process.args)

def ingest_click_house_incremental_load(entity_name):
    import logging
    log = logging.getLogger("airflow.task")

    script_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'dwh', 'ingest_click_house_incremental_load.py'))

    args = ["--entity_name", entity_name]
    process = subprocess.Popen(
        [sys.executable, "-u", script_path] + args,  # -u forces unbuffered output
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1
    )

    for line in process.stdout:
        log.info(line.strip())

    process.wait()
    if process.returncode != 0:
        raise subprocess.CalledProcessError(process.returncode, process.args)

def preprocessing_batch_sources_browsing_history():
    import logging
    log = logging.getLogger("airflow.task")

    script_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'dwh', 'preprocessing_batch_sources_browsing_history.py'))

    # args = ["--entity_name", entity_name]
    args = []
    process = subprocess.Popen(
        [sys.executable, "-u", script_path] + args,  # -u forces unbuffered output
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1
    )

    for line in process.stdout:
        log.info(line.strip())

    process.wait()
    if process.returncode != 0:
        raise subprocess.CalledProcessError(process.returncode, process.args)

def preprocessing_batch_sources_users():
    import logging
    log = logging.getLogger("airflow.task")

    script_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'dwh', 'preprocessing_batch_sources_users.py'))

    # args = ["--entity_name", entity_name]
    args = []
    process = subprocess.Popen(
        [sys.executable, "-u", script_path] + args,  # -u forces unbuffered output
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1
    )

    for line in process.stdout:
        log.info(line.strip())

    process.wait()
    if process.returncode != 0:
        raise subprocess.CalledProcessError(process.returncode, process.args)

def preprocessing_streaming_full_load(entity_name):
    import logging
    log = logging.getLogger("airflow.task")

    script_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'dwh', 'preprocessing_streaming_full_load.py'))

    args = ["--entity_name", entity_name]
    process = subprocess.Popen(
        [sys.executable, "-u", script_path] + args,  # -u forces unbuffered output
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1
    )

    for line in process.stdout:
        log.info(line.strip())

    process.wait()
    if process.returncode != 0:
        raise subprocess.CalledProcessError(process.returncode, process.args)

def preprocessing_streaming_incremental_load():
    import logging
    log = logging.getLogger("airflow.task")

    script_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'dwh', 'preprocessing_streaming_incremental_load.py'))

    args = []
    process = subprocess.Popen(
        [sys.executable, "-u", script_path] + args,  # -u forces unbuffered output
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1
    )

    for line in process.stdout:
        log.info(line.strip())

    process.wait()
    if process.returncode != 0:
        raise subprocess.CalledProcessError(process.returncode, process.args)

default_args = {
    'owner': 'airflow',
    'email': ['dbpedia.rs@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

# Define the DAG
with DAG(
    dag_id="dwh_dag",
    default_args=default_args,
    start_date=datetime(2025, 4, 11),
    # schedule_interval="@daily",
    schedule_interval=None,
    catchup=False,
    concurrency=10,
    max_active_runs=5,
    tags=["dwh"]
) as dag:
    with TaskGroup(group_id="00fs_01land") as ingestion_group:
        with TaskGroup(group_id="batch_sources") as batch_sources:
            run_junyi_Exercise_table_trans = PythonOperator(
                task_id="junyi_Exercise_table_trans",
                python_callable=ingest_batch_sources_full_load,
                op_args=["batch-sources/junyi/junyi_Exercise_table_trans.csv"]
            )

            run_junyi_ProblemLog_original = PythonOperator(
                task_id="junyi_ProblemLog_original",
                python_callable=ingest_batch_sources_full_load,
                op_args=["batch-sources/junyi/sample_1_of_20_junyi_ProblemLog_original.csv"]
            )

        with TaskGroup(group_id="stream_sources") as batch_sources:
            run_click_house_options = PythonOperator(
                task_id="click_house_options",
                python_callable=ingest_click_house_full_load,
                op_args=["options"]
            )

            run_click_house_questions = PythonOperator(
                task_id="click_house_questions",
                python_callable=ingest_click_house_full_load,
                op_args=["questions"]
            )

            run_click_house_users = PythonOperator(
                task_id="click_house_users",
                python_callable=ingest_click_house_incremental_load,
                op_args=["users"]
            )
            run_click_house_browsinghistory = PythonOperator(
                task_id="click_house_browsinghistory",
                python_callable=ingest_click_house_incremental_load,
                op_args=["browsinghistory"]
            )
    with TaskGroup(group_id="01land_02bronze") as preprocessing_group:
        with TaskGroup(group_id="batch_sources_preprocessing") as batch_sources_preprocessing:
            run_preprocessing_batch_sources_stage1= PythonOperator(
                task_id="preprocessing_browsing_history",
                python_callable=preprocessing_batch_sources_browsing_history,
                op_args=None
            )
            run_preprocessing_batch_sources_users= PythonOperator(
                task_id="preprocessing_users",
                python_callable=preprocessing_batch_sources_users,
                op_args=None
            )
        with TaskGroup(group_id="streaming_sources_preprocessing") as streaming_sources_preprocessing:
            run_preprocessing_streaming_sources_options= PythonOperator(
                task_id="preprocessing_streaming_full_load_options",
                python_callable=preprocessing_streaming_full_load,
                op_args=["options"]
            )
            run_preprocessing_streaming_sources_questions= PythonOperator(
                task_id="preprocessing_streaming_full_load_questions",
                python_callable=preprocessing_streaming_full_load,
                op_args=["questions"]
            )
            run_preprocessing_streaming_sources_incremental_load= PythonOperator(
                task_id="preprocessing_streaming_incremental_load",
                python_callable=preprocessing_streaming_incremental_load,
                op_args=[]
            )

[run_junyi_Exercise_table_trans, run_junyi_ProblemLog_original ] >> batch_sources_preprocessing
        
run_click_house_options >> run_preprocessing_streaming_sources_options
run_click_house_questions >> run_preprocessing_streaming_sources_questions
[run_click_house_users, run_click_house_browsinghistory ] >> run_preprocessing_streaming_sources_incremental_load
