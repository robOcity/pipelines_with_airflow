from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="",
        aws_credentials_id="",
        table="",
        s3_bucket="",
        s3_key="",
        role_arn="",
        aws_region="",
        *args,
        **kwargs,
    ):
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_path = f"s3://{s3_bucket}/{s3_key}"
        self.role_arn = role_arn
        self.aws_region = aws_region
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

    def build_cmd(self):
        copy_cmd = f"""
        COPY {self.table}
        FROM '{self.s3_src}'
        IAM_ROLE '{self.role_arn}'
        JSON '{self.s3_dest}' truncatecolumns
        TIMEFORMAT 'epochmillisecs'
        REGION '{self.aws_region}'
        COMPUPDATE off
        MAXERROR 3;
        """
        self.log.info(f"Copy command: {copy_cmd}")
        return copy_cmd

    def execute(self, context):
        self.log.info(
            "\n*** StageToRedshiftOperator: Copying data from S3 JSON files to Redshift ***"
        )
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Dropping {self.table} table from Redshift")
        redshift.run(f"DROP TABLE IF EXISTS {self.table}")

        self.log.info(f"Copying data from {s3_path} to Redshift {self.table} table")
        redshift.run(self.build_cmd())
        self.log.info("*** StageToRedshiftOperator: Complete ***\n")
