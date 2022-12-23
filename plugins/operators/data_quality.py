from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    The DataQualityOperator is used to create an airflow operator that checks for any data quality issues 
    to ensure the dag operations (staging and loading) are completed successfully. otherwise, it rises errors accordingly.

    Note: now, it checks only if a table has rows or not.

    Attributes
    ----------
    conn_id : str
        a redshift connection id stored in airflow variables
    tables : list
        list of table names to apply data quality checks on
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 conn_id = '',
                 tables = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.conn_id = conn_id
        self.tables = tables

    def execute(self, context):
        # create postgres hook to run sql statements
        postgres_hook = PostgresHook(postgres_conn_id=self.conn_id)  
        
        # apply data quality check
        for table in self.tables:
            records = postgres_hook.get_records(f"SELECT COUNT(*) FROM {table}")  
            
            # check if result is retured
            if len(records) < 1 or len(records[0]) < 1:
                self.log.error(f"Number of rows counting operation for {table} table return no results")
                raise ValueError(f"Data quality check failed for {table} table. No results retured")
                
            # check if number of records is zero
            num_records = records[0][0]
            if num_records < 1:
                self.log.error(f"No records exist in {table} table, contains 0 records")
                raise ValueError(f"Data quality check failed for {table} table. No records existed, contains 0 records")
                
            self.log.info(f"Data quality check on {table} table passed with {num_records:,} records")