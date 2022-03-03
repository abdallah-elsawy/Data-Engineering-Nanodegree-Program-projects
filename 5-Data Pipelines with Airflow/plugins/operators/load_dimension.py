from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    To load data from Redshift into dimension tables.
      
    """

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="", 
                 tables="",
                 sql_query="",
                 truncate_table="",
                 *args, **kwargs):


        super(LoadDimensionOperator, self).__init__(*args, **kwargs)

        self.tables = tables
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.truncate_table = truncate_table



    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.truncate_table:
            self.log.info("Delete {} Table" .format(self.tables))
            redshift.run("Delet From {}".format(self.tables))

        self.log.info(f'Running sql_query {self.sql_query}')
        redshift.run(f"Insert into {self.tables} {self.sql_query}")
