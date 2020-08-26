from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook

import json
import io
import contextlib import closing


class S3ToPostgresOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 s3_conn_id=None,
                 s3_bucket=None,
                 s3_directory='',
                 postgres_conn_id='postgres_default',
                 header=False,
                 schema='public',
                 table='raw_load',
                 get_latest=False,
                 *args, **kwargs):

        super(S3ToPostgresOperator, self).__init__(*args, **kwargs)

        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_directory = s3_directory
        self.postgres_conn_id = postgres_conn_id
        self.header = header
        self.schema = schema
        self.table = table
        self.get_latest = get_latest


    def execute(self, context):
        self.log.info('Retrieving credentials')
        s3_hook = S3Hook(self.s3_conn_id)
        s3_session = s3_hook.get_session()
        s3_client = s3_session.client('s3')

        if self.get_latest == True:
            objects = s3_client.list_objects_v2(Bucket=self.s3_bucket, Prefix=self.s3_directory)['Contents']
            latest = max(objects, key=lambda x: x['LastModified'])
            s3_obj = s3_client.get_object(Bucket=self.s3_bucket, Key=latest['Key'])

        file_content = s3_obj['Body'].read().decode('utf-8')

        pg_hook = PostgresHook(self.postgres_conn_id)

        self.log.info('Inserting json object')
        json_content = json.loads(file_content)

        schema = self.schema
        if isinstance(self.schema, tuple):
            schema = self.schema[0]

        table = self.table
        if isinstance(self.table, tuple):
            table = self.table[0]

        target_fields = [unique_id, created_date, closed_date, resolution_action_updated_date, resolution_description,
                        complaint_type, descriptor, status, open_data_channel_type, agency, agency_name, bbl,
                        incident_address, city, borough, community_board, incident_zip, latitude, longitude]
        target_fields = ','.join(target_fields)

        with closing(pg_hook.get_conn()) as conn:
            with closing(conn.cursor()) as cur:
                cur.executemany(
                    f'''INSERT INTO {schema}.{table} ({target_fields})
                    VALUES(
                        %(unique_id)s, %(created_date)s, %(closed_date)s, %(resolution_action_updated_date)s, %(resolution_description)s,
                        %(complaint_type)s, %(descriptor)s, %(status)s, %(open_data_channel_type)s, %(agency)s, %(agency_name)s, %(bbl)s,
                        %(incident_address)s, %(city)s, %(borough)s, %(community_board)s, %(incident_zip)s, %(latitude)s, %(longitude)s
                        );
                    ''',({
                        'unique_id': line['unique_id'], 'created_at': line['created_at'], 'closed_date': line.get('closed_at', None), 'resolution_action_updated_date': line.get('resolution_action_updated_date', None), 'resolution_description' : line.get('resolution_description', None),
                        'complaint_type': line.get('complaint_type', None), 'descriptor': line.get('descriptor', None), 'status': line.get('status', None), 'open_data_channel_type': line.get('open_data_channel_type', None), 'agency': line.get('agency', None), 'agency_name': line.get('agency_name', None), 'bbl': line.get('bbl', None),
                        'incident_address': line.get('incident_address', None), 'city': line.get('city', None), 'borough': line.get('borough', None), 'community_boad': line.get('community_board', None), 'incident_zip': line.get('incident_zip', None), 'latitude': line.get('latitude', None), 'longitude': line.get('longitude', None)
                    } for line in json_content)
                ).conn.commit()

        self.log.info('Insertion complete')