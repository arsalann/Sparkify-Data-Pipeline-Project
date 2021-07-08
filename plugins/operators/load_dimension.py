from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers import SqlQueries

'''
Fact and Dimension Operators

With dimension and fact operators, you can utilize the provided SQL helper class to run data transformations. Most of the logic is within the SQL transformations and the operator is expected to take as input a SQL statement and target database on which to run the query against. You can also define a target table that will contain the results of the transformation.

Dimension loads are often done with the truncate-insert pattern where the target table is emptied before the load. Thus, you could also have a parameter that allows switching between insert modes when loading dimensions. Fact tables are usually so massive that they should only allow append type functionality.
'''

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 table="",
                 redshift_conn_id="",
                 sql="",
                 truncate="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table=table
        self.redshift_conn_id=redshift_conn_id
        self.sql=sql
        self.truncate=truncate

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        formatted_sql = getattr(SqlQueries,self.sql)

        if self.truncate:
            self.log.info("Emptying Dimension Table: {}".format(self.table))
            self.log.info('    Truncating Dimension tables...')
            redshift.run("TRUNCATE {}".format(self.table))
            
            self.log.info('    Loading Dimension tables...')
            redshift.run(formatted_sql)
            self.log.info('\nDimension tables loaded!\n')
        else:
            self.log.info("Emptying Dimension Table: {}".format(self.table))
            self.log.info('    Appending Dimension tables...')
            redshift.run(formatted_sql)
            self.log.info('\nDimension tables appended!\n')
        