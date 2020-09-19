from datetime import datetime
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator

args = {
    'owner': 'HDL Project',
    'start_date': datetime(2020, 5, 1)
}

dag = DAG('postgres_simple_copy',
          schedule_interval='@monthly',
          default_args=args,
          tags=['hdl_project', 'postgres'])


def copy_db():
    user_manager = PostgresHook(postgres_conn_id='user_manager')
    user_manager_conn = user_manager.get_conn()
    user_manager_cursor = user_manager_conn.cursor()

    copy_user_manager = PostgresHook(postgres_conn_id='copy_user_manager')
    copy_user_manager_conn = copy_user_manager.get_conn()
    copy_user_manager_cursor = copy_user_manager_conn.cursor()

    copy_user_manager_cursor.execute('SELECT MAX(id) FROM users_user;')
    last_migrated_id = copy_user_manager_cursor.fetchone()[0]
    if last_migrated_id is None:
        last_migrated_id = 0

    user_manager_cursor.execute('SELECT * FROM users_user WHERE id > %s;', [last_migrated_id])
    copy_user_manager.insert_rows(table='users_user', rows=user_manager_cursor)


run_this = PythonOperator(
    task_id='copy_users_user_table',
    python_callable=copy_db,
    dag=dag,
)
