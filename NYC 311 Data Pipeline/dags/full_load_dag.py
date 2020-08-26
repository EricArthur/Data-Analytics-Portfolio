from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from operators.query_socrata import QuerySocrataOperator
from operators.split_file_to_directory import SplitFileToDirectoryOperator
from operators.save_directory_to_s3 import SaveDirectoryToS3Operator
from operators.s3_to_postgres import S3ToPostgresOperator
from airflow.utils.dates import days_ago
from datetime import timedelta


soda_headers = {
    'keyId': '############',
    'keySecret': '#################',
    'Accept': 'application/json'
}

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=30)
}

with DAG('311_data_full_load',
         default_args=default_args,
         description='Executes full load from Socrata API into Production DWH.',
         max_active_runs=1,
         schedule_interval=None
        ) as dag:

        op1 = QuerySocrataOperator(
            task_id='Query_nyc_open_data',
            dag=dag,
            provide_context=True,
            socrata_domain='data.cityofnewyork.us',
            socrata_dataset_identifier='fhrw-4uyv',
            socrata_token=config['SOCRATA']['API_TOKEN'],
            json_output_filepath='nyc_311_{yesterday_ds}.json',
            socrata_query_filters={
                'where': "closed_date BETWEEN {yesterday_ds} AND {ds}",
                'limit': 1000000
            }
        )

        op2 = SplitFileToDirectoryOperator(
            task_id='Split_nyc_311_json_data',
            dag=dag,
            provide_context=True,
            json_input_filepath='nyc_311_{yesterday_ds}.json',
            output_directory='nyc_311_{yesterday_ds}/',
            json_output_filepath='{unique_id}.json'
        )

        op3 = SaveDirectoryToS3Operator(
            task_id='Save_nyc_open_data_to_S3',
            dag=dag,
            provide_context=True,
            s3_conn_id='S3_311_NYC',
            s3_bucket='nyc-311-data',
            s3_directory='{execution_date.year}/{execution_date.month}/{execution_date.day}/',
            local_directory='nyc_311_{yesterday_ds}/',
            replace=True
        )

        op4 = PostgresOperator(
            task_id='initialize_target_db',
            postgres_conn_id='RDS_311_NYC',
            sql='sql/init_db_schema.sql',
            dag=dag
        )

        op5 = S3ToPostgresOperator(
            task_id='load_311_data',
            s3_conn_id='S3_311_NYC',
            s3_bucket='nyc-311-data',
            s3_directory='soda_jsons/soda',
            source_data_type='json',
            postgres_conn_id='RDS_311_NYC',
            schema='raw',
            table='service_request',
            get_latest=True,
            dag=dag
        )

        op6 = PostgresOperator(
            task_id='execute_full_load',
            postgres_conn_id='RDS_311_NYC',
            sql='sql/full_load.sql',
            dag=dag
        )

        op1 >> op2 >> op3 >> op4 >> op5 >> op6