from airflow import DAG

from datetime import datetime
from airflow.operators.python import PythonOperator

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipelines.wikipedia_pipeline import extract_wiki_data, transform_wiki_data, write_wiki_data



dag = DAG(
    dag_id="wikipedia_flow",
    default_args={
        'owner': "TriDoan",
        'start_date': datetime(2024, 7, 17)
    },
    schedule_interval=None,
    catchup=False
)

# extract

extract_from_wikipedia = PythonOperator(
    task_id = "extract_data_from_wikipedia",
    python_callable= extract_wiki_data,
    provide_context=True,
    op_kwargs={
        "url": "https://en.wikipedia.org/wiki/List_of_association_football_stadiums_by_capacity",
    },
    dag=dag
)

# transform
transform_wikipedia_data =PythonOperator(
    task_id = "transform_data_from_wikipedia",
    provide_context=True,
    python_callable= transform_wiki_data,
    dag= dag
)


# write 
write_wikipedia_data = PythonOperator (
    task_id = "write_wikipedia_data",
    provide_context = True,
    python_callable= write_wiki_data,
    dag = dag
)

extract_from_wikipedia >> transform_wikipedia_data >> write_wikipedia_data