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


def copy_table(src_conn_id, dst_conn_id, table):
    src = PostgresHook(postgres_conn_id=src_conn_id)
    src_conn = src.get_conn()
    src_cursor = src_conn.cursor()

    dst = PostgresHook(postgres_conn_id=dst_conn_id)
    dst_conn = dst.get_conn()
    dst_cursor = dst_conn.cursor()

    dst_cursor.execute('SELECT MAX(id) FROM %s;', [table])
    last_migrated_id = dst_cursor.fetchone()[0]
    if last_migrated_id is None:
        last_migrated_id = 0

    src_cursor.execute('SELECT * FROM %s WHERE id > %s;', [table, last_migrated_id])
    dst.insert_rows(table=table, rows=src_cursor)


copy_users_user_table = PythonOperator(
    task_id='copy_users_user_table',
    python_callable=copy_table,
    op_kwargs={
        'src_conn_id': 'user_manager',
        'dst_conn_id': 'copy_user_manager',
        'table': 'users_user'
    },
    dag=dag,
)
