from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 postgres_conn_id = "",
                 sql= "",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.sql = sql

    def execute(self, context):
        redshift_hook = PostgresHook(self.postgres_conn_id)
        self.log.info('LoadFactOperator Operator sql:', self.sql)

        redshift_hook.run(self.sql)
