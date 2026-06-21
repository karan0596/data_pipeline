from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    # @apply_defaults
    def __init__(self,
                 redshift_conn_id = '',
                 table = '',
                 sql = '',
                 **kwargs):

        super(LoadFactOperator, self).__init__(**kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql

    def execute(self, context):
        self.log.info(f'Loading {self.table} fact table.')
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        redshift_hook.run(self.sql)
        self.log.info(f'Fact table {self.table} loaded successfully.')

