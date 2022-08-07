from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 dq_checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.dq_checks=dq_checks

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        for dq_check in dq_checks:
            records = redshift.get_records(dq_check['check_sql'])  

            if len(reconrds) != dq_check['expected_result']:
                self.log.info(f"Quality checks failed with SQL {dq_check['check_sql']}")
            else:
                self.log.info(f"Data quality check passed with {len(records)} records")
            
        
        