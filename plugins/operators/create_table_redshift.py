import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook 


class CreateTablesOnRedshiftOperator(BaseOperator):

    template_fields= ("create_sql",)

    drop_tables_sql = """
        DROP TABLE IF EXISTS {}
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 create_sql="",
                 *args, **kwargs):
        
        super(CreateTablesOnRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.create_sql = create_sql

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Creating tables on Redshift...")
        redered_create_sql = self.create_sql.format(**context)
        formated_drop_sql = CreateTablesOnRedshiftOperator.drop_tables_sql.format(
            self.table
        )
        redshift_hook.run(formated_drop_sql)
        redshift_hook.run(redered_create_sql)