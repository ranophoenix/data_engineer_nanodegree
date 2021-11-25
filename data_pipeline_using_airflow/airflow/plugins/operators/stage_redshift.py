from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    template_fields = ("s3_key",)   

    @apply_defaults
    def __init__(self,
                 table,
                 s3_key,                 
                 s3_bucket="udacity-dend",                 
                 redshift_conn_id="redshift",
                 aws_credentials_id="aws_credentials",                                                
                 file_format="json",
                 json_path="auto",
                 delimiter=",",
                 ignore_headers=1,
                 region='us-west-2',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.s3_key = s3_key        
        self.s3_bucket = s3_bucket        
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id                                 
        self.file_format = file_format
        self.json_path = json_path
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers
        self.region = region
        

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)        
        
        self.log.info(f'Truncating table {self.table}')
        redshift.run(f'TRUNCATE TABLE {self.table}')        
        
        self.log.info("Copying data from S3...")
        if self.file_format == 'csv':
            format_command = f" IGNOREHEADER '{self.ignore_header}' DELIMITER '{self.delimiter}'"
        else:
            format_command = f" JSON '{self.json_path}'"
            
        key_from_template = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{key_from_template}"
        
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        
        redshift.run(f"""         
            COPY {self.table}
            FROM '{s3_path}'
            ACCESS_KEY_ID '{credentials.access_key}'
            SECRET_ACCESS_KEY '{credentials.secret_key}'
            REGION '{self.region}'
            {format_command}        
        """)
        




