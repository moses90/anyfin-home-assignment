from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime, timedelta

# Define default arguments for the DAG
default_args = {
    'owner': 'mozes',
    'depends_on_past': False,
    'start_date': datetime(2023, 11, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create a DAG instance
dag = DAG(
    'task5__encryption_creation',
    default_args=default_args,
    schedule_interval='@daily',  # Run daily
    catchup=False,
    max_active_runs=1,
)

# Define SQL queries for table deletion and creation
create_encryption_extension = "create extension if not exists pgcrypto;"
delete_table_sql = "drop table if exists task5_datamodel restrict;"
create_table_sql = """
create table task5_datamodel as (
	select distinct
		c.customer_id,
		a.email,
		pgp_sym_encrypt(a.email,'anyfin_home_assignment') as encrypted_email
	from cycles as c
	left join applications as a
	on c.customer_id = a.customer_id
	where dpd > 10
);
"""

# Task to create extension if doesn't exist
create_extension_task = PostgresOperator(
    task_id='encryption_extension',
    postgres_conn_id='anyfin_postgres_connection',
    sql=delete_table_sql,
    dag=dag,
    autocommit=True,
)

# Task to delete the table if it exists
delete_table_task = PostgresOperator(
    task_id='delete_table',
    postgres_conn_id='anyfin_postgres_connection',
    sql=delete_table_sql,
    dag=dag,
    autocommit=True,
)

# Task to create a new table
create_table_task = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='anyfin_postgres_connection',
    sql=create_table_sql,
    dag=dag,
    autocommit=True,
)

# Set task dependencies
create_extension_task >> delete_table_task >> create_table_task

# if __name__ == "__main__":
#     dag.cli()
