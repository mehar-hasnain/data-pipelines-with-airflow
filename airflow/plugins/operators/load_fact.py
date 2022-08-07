from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    
    truncate_sql = """
        TRUNCATE TABLE {};
        """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='redshift',
                 fact_table_name='',
                 fact_insert_sql='',
                 truncate_table=False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.fact_table_name = fact_table_name
        self.fact_insert_sql = fact_insert_sql
        self.truncate_table = truncate_table

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id) 
        
        if self.truncate_table:
            self.log.info(f'Truncating data from {self.table} fact table')
            redshift_hook.run(truncate_sql.format(self.table))
            
        self.log.info(f'Inseting data into {self.table} fact table')
        redshift_hook.run(fact_insert_sql)

