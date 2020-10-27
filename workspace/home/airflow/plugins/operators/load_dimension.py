from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    
    dimension_sql_template = """ 
    INSERT INTO {}
    {}
    """
    

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift", 
                 table="",
                 sql="",
                 append=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.table=table
        self.sql=sql
        self.append=append
        

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if not self.append:            
            self.log.info("Clearing data from {}".format(self.table))
            redshift.run("DELETE FROM {}".format(self.table))   
        
        self.log.info("Running insert statement to {}".format(self.table))
        
        formatted_insert_sql = LoadDimensionOperator.dimension_sql_template.format(self.table, self.sql)
        
        redshift.run(formatted_insert_sql)
        
        