import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import date, timedelta

from crawl import crawl_blogger_articles

# +
default_args = {
    'owner': 'shicheng',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'email': ['shicheng1627@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'blog_article_daily_update', default_args=default_args,
    schedule_interval=timedelta(days=1),
    max_active_runs=1)
# -

t1 = PythonOperator(
    task_id='first_page_crawl',
    python_callable=crawl_blogger_articles,
    op_kwargs={'max_page': 1},
    dag=dag
)
