from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    truncate_dim_sql = """
        TRUNCATE TABLE {};
        """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 load_dim_sql="",
                 truncate_table=False, 
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.load_dim_sql = load_dim_sql
        self.truncate_table = truncate_table

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.truncate_table:
            self.log.info(f'Truncating data from {self.table} dimension table')
            redshift_hook.run(truncate_dim_sql.format(self.table))
            
        
        self.log.info(f"Inserting data into {self.table} dimension table")
        redshift_hook.run(load_dim_sql)
