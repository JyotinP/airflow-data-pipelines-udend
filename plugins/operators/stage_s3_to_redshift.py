import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
# from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.contrib.hooks.aws_hook import AwsHook


class StageS3ToRedshiftOperator(BaseOperator):

    template_fields= ("s3_key",)

    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        json '{}'
    """

    @apply_defaults
    def __init__(self,
                 aws_conn_id= "",
                 s3_bucket = "",
                 s3_key = "",
                 redshift_conn_id="",
                 table="",
                 json="",
                 *args, **kwargs):
        
        super(StageS3ToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.json = json

    def execute(self, context):
        aws_hook = AwsHook(aws_conn_id=self.aws_conn_id)
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        redered_s3_key = self.s3_key.format(**context)
        self.log.info("Copying data from S3 to Staging tables on Redshift...")
        s3_path = f"s3://{self.s3_bucket}/{redered_s3_key}"
        formated_sql = StageS3ToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json
        )
        redshift_hook.run(formated_sql)


