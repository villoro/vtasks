"""
    DAG operator for expensor
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

from expensor import process
from expensor.config import PATH_ROOT

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2019, 4, 19),
    "email": ["villoro7@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG("expensor", default_args=default_args, schedule_interval=timedelta(days=1))


fetch_code = f"""
cd {PATH_ROOT}
git fetch
git checkout master
git pull origin master
"""

expensor_fetch_code = BashOperator(
    task_id="expensor_fetch_code", depends_on_past=False, bash_command=fetch_code, dag=dag
)

expensor_operator = PythonOperator(
    task_id="expensor_do_all", provide_context=True, python_callable=process.do, dag=dag
)

# expensor_operator.set_upstream(expensor_fetch_code)
