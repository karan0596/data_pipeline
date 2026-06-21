from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    # @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 tables=None,
                 **kwargs):

        super(DataQualityOperator, self).__init__(**kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables=tables

    def execute(self, context): 
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for test in self.tests:
            self.log.info(f"Running test: {test['check_sql']}")
            result = redshift.get_first(test['check_sql'])

            if result is None or len(result) == 0:
                raise ValueError(f"Test returned no results: {test['check_sql']}")

            # Compare based on test configuration
            if 'expected_value' in test and result[0] != test['expected_value']:
                raise ValueError(
                    f"Test failed: {test['check_sql']}. "
                    f"Expected {test['expected_value']}, got {result[0]}"
                )

            if 'expected_min' in test and result[0] < test['expected_min']:
                raise ValueError(
                    f"Test failed: {test['check_sql']}. "
                    f"Expected minimum {test['expected_min']}, got {result[0]}"
                )

            self.log.info(f"Test passed: {test['check_sql']}")