import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
# from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.contrib.hooks.aws_hook import AwsHook


class LoadDimensionFromStageOperator(BaseOperator):

    template_fields= ("load_sql",)

    truncate_tables_sql = """
        TRUNCATE TABLE {}
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 load_sql="",
                 truncate="",
                 *args, **kwargs):
        
        super(LoadDimensionFromStageOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.load_sql = load_sql
        self.truncate = truncate

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Loading Dimension tables from Staging...")
        redered_load_sql = self.load_sql.format(**context)
        formated_truncate_tables_sql = LoadDimensionFromStageOperator.truncate_tables_sql.format(
            self.table
        )
        if self.truncate is True:
            redshift_hook.run(formated_truncate_tables_sql)
        redshift_hook.run(redered_load_sql)