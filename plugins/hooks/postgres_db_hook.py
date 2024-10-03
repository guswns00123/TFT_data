import os

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook
import tempfile
import psycopg2



class PostgresToS3Operator(BaseOperator):
    template_fields = ("_query", "_s3_key")

    def __init__(self, postgres_conn_id,query,s3_conn_id, s3_bucket, s3_key, pd_csv_kwargs, **kwargs):
        self.postgres_conn_id = postgres_conn_id
        self._query = query
        self._s3_conn_id = s3_conn_id
        self._s3_bucket = s3_bucket
        self._s3_key = s3_key
        self._pd_csv_kwargs = pd_csv_kwargs

        if not self._pd_csv_kwargs:
            self._pd_csv_kwargs = {}

        

    def get_conn(self):
        airflow_conn = BaseHook.get_connection(self.postgres_conn_id)
        self.host = airflow_conn.host
        self.user = airflow_conn.login
        self.password = airflow_conn.password
        self.dbname = airflow_conn.schema
        self.port = airflow_conn.port
        

        self.postgres_conn = psycopg2.connect(host=self.host, user=self.user, password=self.password, dbname=self.dbname, port=self.port)
        return self.postgres_conn

    def execute(self, context):
        
        postgres_hook = self.get_conn()
        s3_hook = S3Hook(aws_conn_id=self._s3_conn_id)
        
        data_df = postgres_hook.get_pandas_df(self._query)
        
        with tempfile.NamedTemporaryFile(mode='r+', suffix='.csv') as tmp_csv:
            tmp_csv.file.write(data_df.to_csv(**self._pd_csv_kwargs))
            tmp_csv.file.seek(0)
            s3_hook.load_file(filename=tmp_csv.name,
                            key=self._s3_key,
                            bucket_name=self._s3_bucket)

        if s3_hook.check_for_key(self._s3_key, bucket_name=self._s3_bucket):
            file_location = os.path.join(self._s3_bucket, self._s3_key)
            self.log.info("File saved correctly in %s", file_location)
    
