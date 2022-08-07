from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        FORMAT AS JSON '{}';
    """
    
    truncate_sql = """
        TRUNCATE TABLE {};
        """

    @apply_defaults
    def __init__(self,
                 table="",
                 redshift_conn_id="",
                 aws_credentials_id="",
                 s3_bucket="",
                 s3_key="",
                 region="us-west-2",
                 json_path="",
                 truncate_table=False,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.region = region
        self.json_path = json_path
        self.truncate_table = truncate_table
        

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.truncate_table:
            self.log.info(f"Truncating table {self.table}")
            redshift_hook.run(self.truncate_sql.format(self.table))    

        self.log.info(f"Copying data from S3 to Redshift staging {self.table} table")

        copy_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            self.s3_bucket,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.json_path
        )
        
        self.log.info(f"COPY SQL: {copy_sql}")
        self.log.info(f"Copying data from '{self.s3_bucket}' to '{self.table}'")
        redshift_hook.run(copy_sql)
