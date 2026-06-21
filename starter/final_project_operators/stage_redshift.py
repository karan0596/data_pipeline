from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    # @apply_defaults
    def __init__(self,
                redshift_conn_id='',
                table='',
                s3_bucket='',
                s3_key='',
                iam_role='',
                json_format='auto',
                **kwargs):
        super(StageToRedshiftOperator, self).__init__(**kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.iam_role = iam_role
        self.json_format = json_format

    def execute(self, context):
        self.log.info(f'Staging data from s3://{self.s3_bucket}/{self.s3_key}/ -> {self.table}')

        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f'Deleting {self.table} before staging')
        redshift_hook.run(f'DELETE FROM {self.table}')

        copy_sql = """
        COPY {table}
        FROM '{s3_path}'
        IAM_ROLE '{iam_role}'
        FORMAT AS JSON '{json_format}'
        """

        final_sql = copy_sql.format(
            table=self.table,
            s3_path=f's3://{self.s3_bucket}/{self.s3_key}/',
            iam_role=self.iam_role,
            json_format=self.json_format
        )

        redshift_hook.run(final_sql)
        self.log.info(f'Staging complete.')







