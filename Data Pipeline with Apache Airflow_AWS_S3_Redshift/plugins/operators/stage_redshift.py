from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class StageToRedshiftOperator(BaseOperator):
    template_fields = ("s3_key",)
    ui_color = '#358140'

    copy_query = " COPY {} \
    FROM '{}' \
    ACCESS_KEY_ID '{}' \
    SECRET_ACCESS_KEY '{}' \
    IGNOREHEADER {} \
    FORMAT AS json '{}'; \
    "

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credential_id="",
                 table_name="",
                 s3_bucket="",
                 s3_key="",
                 file_format="",
                 ignore_headers=1,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credential_id = aws_credential_id
        self.table_name = table_name
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.file_format = file_format
        self.ignore_headers = ignore_headers
        self.execution_date = kwargs.get('execution_date')

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credential_id)
        credentials = aws_hook.get_credentials()

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table_name))

        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        self.log.info(
            f"Picking staging file for table {self.table_name} from location : {s3_path}")

        formatted_sql = StageToRedshiftOperator.copy_query.format(
            self.table_name,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.ignore_headers,
            self.file_format
        )
        self.log.info(f"Running copy query : {formatted_sql}")
        redshift.run(formatted_sql)
        self.log.info(f"Table {self.table_name} staged successfully!!")
