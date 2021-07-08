from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

"""
Data Quality Operator

The final operator to create is the data quality operator, which is used to run checks on the data itself. The operator's main functionality is to receive one or more SQL based test cases along with the expected results and execute the tests. For each the test, the test result and expected result needs to be checked and if there is no match, the operator should raise an exception and the task should retry and fail eventually.

For example one test could be a SQL statement that checks if certain column contains NULL values by counting all the rows that have NULL in the column. We do not want to have any NULLs so expected result would be 0 and the test would compare the SQL statement's outcome to the expected result.
"""



class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='redshift',
                 sql_check_queries=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.sql_check_queries = sql_check_queries
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        
        for table in self.tables:
            self.log.info("Data Quality Starting...")

            self.log.info("\n\n1) General Data Quality Check for Table: {} Starting...".format(table))
            
            record = redshift.get_records("SELECT * FROM {} LIMIT 1".format(table))
            records = redshift.get_records("SELECT COUNT(*) FROM {}".format(table))
            num_records = records[0][0]
            
            if len(records) < 1 or len(records[0]) < 1:
                self.log.error("Data Quality Failure for Table: {}".format(table))
                raise ValueError("{} First Record: {}".format(table, record[0]))
                
            if num_records == 0:
                self.log.error("Data Quality Failure for Table: {}".format(table))
                raise ValueError("{} Number of Records: {}".format(table, num_records))

            self.log.info("\nTable {} Data Quality Check Complete. See below details:".format(table))
            self.log.info("    First Record: {}".format(record[0]))
            self.log.info("    Total Number of Records: {}".format(num_records))

            self.log.info("\nGeneral Data Quality Check for Table: {} Complete!".format(table))


        errors_total = 0
        errors_query = []

        for query in self.sql_check_queries:

            sql = query.get('sql')
            result = query.get('result')
            self.log.info("\n\n2) Custom Data Quality Check Starting...")
            self.log.info("    Data Quality Query: {}".format(self.sql))
            self.log.info("    Expected Result: {}".format(self.result))
            
            records = redshift.get_records(sql)[0][0]

            if result != records:
                errors_total += 1
                errors_query.append(sql)

        if errors_total > 0:
            raise ValueError('Total Number of Failed Queries = {}'.format(errors_total))
            raise ValueError('The following queries failed: ')
            self.log.info(errors_query)

        else:
            self.log.info("\nCustom Data Quality Checks Complete!")