from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityCheckOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 checks ="",
                 *args, **kwargs):
        
        super(DataQualityCheckOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.checks = checks
        
    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Data Quality Check is in progress...")
        for i, v in enumerate(self.checks):
             records = redshift_hook.get_records(sql = v['sql'])
             if records[0][0]  == v['expected']:
                self.log.info(f"{v['title']} passed with {records[0][0]} matching expected {v['expected']}")
             else:
                self.log.info(f"{v['title']} failed with {records[0][0]}\
                        records, did not match with expected {v['expected']}")
                raise ValueError(f" {v['title']} failed having {records[0][0]} records")