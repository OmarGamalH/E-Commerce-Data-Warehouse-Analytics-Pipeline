import airflow.sdk as sdk
import Utilities as ut
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow import DAG
import datetime

all_tables = ['customers' , 'products' , 'category_name' , 'geolocation' , 'order_items' , 'order_payments' , 'order_reviews' , 'orders' , 'sellers']
all_tasks = []

@sdk.dag(
    dag_id = 'e_commerece_dag',
    schedule = "0 * * * *", 
    start_date = datetime.datetime(year = 2026 , month = 9 , day = 1) 
)

def e_commerce_dag():

    start = EmptyOperator(task_id = 'start')

    for table in all_tables:
        @sdk.task_group(group_id = f'{table}_task_group')
        def task_group():

            delete_data =  PythonOperator(
                    task_id = f'{table}_delete_data',
                    python_callable = ut.delete_data,
                    op_kwargs={'cursor' : ut.cursor , 'table_name' : f'{table}'}
            )

            from_csv_to_snowflake = PythonOperator(
                task_id = f'{table}_from_csv_to_snowflake',
                python_callable = ut.from_csv_to_snowflake,
                op_kwargs= {'conn' : ut.conn , 'name' : f'{table}'} 
            )

            delete_data >> from_csv_to_snowflake    

        all_tasks.append(task_group())


    end = EmptyOperator(task_id = 'end')


    dbt_run = BashOperator(
        task_id = "dbt_run" , bash_command = "cd /opt/airflow/dags/e_commerce_dbt && dbt run && dbt snapshot")


    
    start  >>  all_tasks >> dbt_run >> end 
    
    

e_commerce_dag()


