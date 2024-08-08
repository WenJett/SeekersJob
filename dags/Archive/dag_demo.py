from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 12, 4),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=10)
}

mock_order_data = {
    'order_id': 0,
    'customer_id': 123,
    'amount': 100.0,
    'status': 'completed'
}


def extract_order_data():
    # by default, any operator that return a value will push it to xcom
    return mock_order_data


def transform_order_data(ti):
    order_data_dict = ti.xcom_pull(task_ids='extract')
    order_data_dict['amount'] = order_data_dict['amount'] * 1.1  # Apply a tax
    # pushes the transformed data to xcom
    return order_data_dict


with DAG('ecommerce_etl', default_args=default_args, schedule=None, catchup=False, tags=['tutorial5']) as dag:

    # the SQLExecuteQueryOperator is an operator that wraps the PostgresHook
    t0 = SQLExecuteQueryOperator(
        task_id='create_table',
        conn_id='postgres_t5',
        sql="""
        CREATE TABLE IF NOT EXISTS orders (
            order_id INTEGER PRIMARY KEY,
            customer_id INTEGER NOT NULL,
            amount FLOAT NOT NULL,
            status VARCHAR(50) NOT NULL
        );
        """,
        autocommit=True,
    )

    t1 = PythonOperator(
        task_id='extract',
        python_callable=extract_order_data,
    )

    t2 = PythonOperator(
        task_id='transform',
        python_callable=transform_order_data,
    )

    t3 = SQLExecuteQueryOperator(
        task_id='load_order_data',
        conn_id='postgres_t5',
        sql="""
        INSERT INTO orders (order_id, customer_id, amount, status)
        VALUES (%(order_id)s, %(customer_id)s, %(amount)s, %(status)s);
        """,
        autocommit=True,
        parameters={
            "order_id": "{{ ti.xcom_pull(task_ids='transform')['order_id'] }}",
            "customer_id": "{{ ti.xcom_pull(task_ids='transform')['customer_id'] }}",
            "amount": "{{ ti.xcom_pull(task_ids='transform')['amount'] }}",
            "status": "{{ ti.xcom_pull(task_ids='transform')['status'] }}",
        },
    )

    t0 >> t1 >> t2 >> t3
