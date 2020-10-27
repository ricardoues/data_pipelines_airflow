from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift", 
                 sql = [], 
                 expected_results = [],                
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.sql=sql
        self.expected_results=expected_results
        

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if len(self.sql) != len(self.expected_results):
            raise ValueError(f"sql list must have the same length as expected_results list.")
        
        for i in range(len(self.sql)):
            records = redshift.get_records(self.sql[i])
                        
            
            if len(records) < 1 or len(records[0]) < 1:
                self.log.info("Query failed {}".format(self.sql[i]))
                raise ValueError(f"Data quality check failed.Returned no results")
            
            num_records = records[0][0]
            if num_records != self.expected_results[i]:
                self.log.info("Query failed{}". format(self.sql[i]))
                raise ValueError(f"Data quality check failed. The number of records do not match with the expected results.")
        
        self.log.info(f"Data quality check passed.")            
            
            