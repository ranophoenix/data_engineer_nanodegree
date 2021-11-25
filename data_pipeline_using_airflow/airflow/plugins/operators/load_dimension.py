from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 table,
                 sql,
                 append=False,
                 redshift_conn_id="redshift",                 
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.sql = sql
        self.append = append
        self.redshift_conn_id = redshift_conn_id
        

    def execute(self, context):
        self.log.info(f'LoadDimensionOperator: loading {self.table}. Append: {self.append}')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if not self.append:
            redshift.run(f"TRUNCATE TABLE {self.table}")
        redshift.run(f"INSERT INTO {self.table} {self.sql}")
            
