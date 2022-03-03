from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}' 
        FORMAT as json '{}'
    """
        
    @apply_defaults
    def __init__(self, 
                 redshift_conn_id="",
                 aws_credentials_id="", 
                 tables="",
                 s3_bucket="",
                 s3_key="",
                 json_path_option="auto",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.tables = tables
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path_option = json_path_option

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        self.log.info(f"Delete data from destination {self.tables} table on Redshift")
        redshift.run("Delete From {}".format(self.tables))


        if self.truncate_table:
            self.log.info("Delete {} Table" .format(self.tables))
            redshift.run("Delete From {}".format(self.tables))

        self.log.info(f'Running sql_query {self.sql_query}')
        redshift.run(f"Insert into {self.tables} {self.sql_query}")


       
        self.log.info("Copy from S3 to RedShift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.tables,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json
        )

        redshift.run(formatted_sql)
        self.log.info("Copying completed ")