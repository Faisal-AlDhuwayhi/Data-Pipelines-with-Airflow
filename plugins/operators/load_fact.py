from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    The LoadFactOperator is used to create an airflow operator that load data from staging tables
    to a fact table in redshift.

    Attributes
    ----------
    conn_id : str
        a redshift connection id stored in airflow variables
    table : str
        the table name where the data will be inserted 
    sql : str
        the sql statement needed for loading
    truncate_flag : str
        whether to truncate any data in the table or not before loading (default=False)
    """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 conn_id = "",
                 table = "",
                 sql = "",        
                 truncate_flag = False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.conn_id = conn_id
        self.table = table
        self.sql = sql
        self.truncate_flag = truncate_flag

    def execute(self, context):
        # create postgres hook to run sql statements
        postgres_hook = PostgresHook(postgres_conn_id=self.conn_id)
        
        # clear the fact table
        if self.truncate_flag:
            self.log.info(f'Clearing (TRUNCATE) {self.table} fact table')
            postgres_hook.run(f'TRUNCATE {self.table}')
            
        # insert the data from staging tables into a fact table
        self.log.info(f"Inserting data from staging tables into {self.table} fact table")
        inser_stmt = f"INSERT INTO {self.table} {self.sql}"
        postgres_hook.run(inser_stmt)
        
        self.log.info(f"Successfully finished insert into {self.table} fact table")
