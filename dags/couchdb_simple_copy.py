from datetime import datetime
from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.http_hook import HttpHook
import json

args = {
    'owner': 'HDL Project',
    'start_date': datetime(2020, 8, 1)
}
DAG_NAME = 'couchdb_simple_copy'

dag = DAG(DAG_NAME,
          schedule_interval='@monthly',
          default_args=args,
          tags=['hdl_project', 'couchdb'],
          max_active_runs=100)

get_product_catalog_db = SimpleHttpOperator(
    task_id='get_product_catalog_db',
    endpoint='product/_design/product/_view/by_type_subtype_id',
    method='GET',
    xcom_push=True,
    http_conn_id='product_catalog',
    dag=dag,
)


def store_product_catalog_with_partition(**kwargs):
    task_instance = kwargs['ti']
    get_product_catalog_db_response = task_instance.xcom_pull(key=None, task_ids='get_product_catalog_db')
    get_product_catalog_db_json = json.loads(get_product_catalog_db_response)

    http_hook = HttpHook(
        method='POST',
        http_conn_id='product_catalog',
    )

    for index, product_catalog in enumerate(get_product_catalog_db_json['rows']):
        product_catalog_document = product_catalog['value']
        product_catalog_document.pop('_rev', None)
        product_catalog_document['_id'] = f"{product_catalog_document['type']}_{product_catalog_document['subtype']}" \
                                          f":{product_catalog_document['_id']}"

        print("request", product_catalog_document)
        print("request", json.dumps(product_catalog_document).encode('utf-8'))
        print("request", json.loads(json.dumps(product_catalog_document)))
        response = http_hook.run(
            endpoint='copy_product',
            headers={"Content-Type": "application/json; charset=utf-8"},
            json=product_catalog_document,
        )

        print("response", response)


store_product_catalog_with_partition = PythonOperator(
    task_id='prepare_product_catalog',
    provide_context=True,
    python_callable=store_product_catalog_with_partition,
    dag=dag,
)

get_product_catalog_db >> store_product_catalog_with_partition

if __name__ == '__main__':
    dag.clear(reset_dag_runs=True)
    dag.run(start_date=args['start_date'])
