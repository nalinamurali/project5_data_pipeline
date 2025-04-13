from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.secrets.metastore import MetastoreBackend
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from udacity.common.final_project_sql_statements import SqlQueries


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    sql_template = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION 'ap-southeast-2'
        TIMEFORMAT as 'epochmillisecs'
        TRUNCATECOLUMNS 
        BLANKSASNULL 
        EMPTYASNULL
        JSON '{}' 
    """


    @apply_defaults
    def __init__(self,
                postgres_conn_id="",
                aws_credentials_id="",
                table="",
                s3_bucket="",
                s3_key="",
                region="",
                json_format ="auto",
                execution_date=None,
                *args, 
                **kwargs
                ):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_format = json_format
        self.execution_date = execution_date

    def execute(self, context):
        self.log.info('************StageToRedshiftOperator is working now************')

        metastoreBackend = MetastoreBackend()
        aws_connection=metastoreBackend.get_connection(self.aws_credentials_id)
        redshift = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        #redshift.run(SqlQueries.COPY_ALL_STAGING_EVENTS_SQL.format(aws_connection.login, aws_connection.password))

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        s3_dir = self.s3_key
        #if self.execution_date:
            # Backfill a specific date
            #year = "2018"
            #month = "11"
            
            #s3_dir = s3_dir.format(year, month)
        s3_path = """s3://{}/{}""".format(self.s3_bucket, s3_dir)

        formated_sql = StageToRedshiftOperator.sql_template.format(
            self.table,
            s3_path,
            aws_connection.login,
            aws_connection.password,
            self.json_format
        )
        formated_sql = formated_sql.replace("\n", "")
        self.log.info("debug sql run:", formated_sql)
        redshift.run(formated_sql)










