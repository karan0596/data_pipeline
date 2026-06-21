from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    # @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 table='',
                 sql='',
                 mode='append',
                 **kwargs):

        super(LoadDimensionOperator, self).__init__(**kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table,
        self.sql = sql,
        self.mode = mode

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.mode == 'delete-load':
            self.log.info(f'Truncating {self.table} data before loading into it.')
            redshift_hook.run(f'TRUNCATE TABLE {self.table}')
        
        self.log.info(f'Loading {self.table} dimension table.')
        redshift_hook.run(self.sql)
        self.log.info(f'Dimension table {self.table} loaded successfully.')
