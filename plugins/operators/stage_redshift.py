from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    """
    The StageToRedshiftOperator is used to create an airflow operator that stages data from s3 (source)
    to redshift (destination) using a templated field that allows it to load timestamped files from S3
    based on the execution time and run backfills.

    Attributes
    ----------
    redshift_conn_id : str
        a redshift connection id stored in airflow variables
    aws_credentials_id : str
        a aws credentials id stored in airflow variables
    table : str
        the table name where the data from s3 will be staged
    s3_bucket : str
        the S3 bucket name that stores the source data
    s3_key : str
        the S3 key that stores the source data
    region: str
        the region where the source data stored
    data_store_format: str
        how the data is formatted inside the s3 bucket to stage it correctly 
    """

    ui_color = '#358140'
    template_fields = ('s3_key',)
    copy_sql_stmt = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        {} REGION '{}'
    """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id='',
                 aws_credentials_id='',
                 table='',
                 s3_bucket='',
                 s3_key='',
                 region='',
                 data_store_format='',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.data_store_format = data_store_format
        
    def execute(self, context):
        # create an aws hook to get aws credentials
        aws_hook = AwsHook(self.aws_credentials_id)
        aws_credentials = aws_hook.get_credentials()
        
        # create a redshift hook to run sql commands
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # clear the staging tables
        self.log.info(f'Clearing (TRUNCATE) Redshift table {self.table}')
        redshift_hook.run(f'TRUNCATE {self.table}')
            
        # format the copy sql statement using the needed params
        rendered_key = self.s3_key.format(**context)
        s3_path = 's3://{}/{}'.format(self.s3_bucket, rendered_key)
        
        formatted_sql = StageToRedshiftOperator.copy_sql_stmt.format(
            self.table, s3_path, aws_credentials.access_key,
            aws_credentials.secret_key, self.data_store_format, self.region
        )
        
        # run the copying command
        self.log.info(f'Copying data from {s3_path} to Redshift target table {self.table}')
        redshift_hook.run(formatted_sql)
        
        self.log.info(f"Successfully finished copy to {self.table}")
            
            
        
        
        





