from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    ui_color = "#89DA59"

    @apply_defaults
    def __init__(self, dq_checks, redshift_conn_id="redshift", *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.dq_checks = dq_checks
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for check in self.dq_checks:
            sql = check.get("check_sql")
            fn_check = check.get("fn_check_result")
            result = redshift_hook.get_records(sql)[0]
            if not fn_check(result):
                raise ValueError(f"Data quality check failed: Table {sql}.")
            self.log.info(f"Data quality '{sql}' check passed with {result} records")
