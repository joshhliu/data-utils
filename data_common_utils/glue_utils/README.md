```
from data_common_utils.glue_utils.source_stage import SyncConnections, SyncTables, GlueLoad, InitializeVariables

###### Glue Variables ######
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
args = getResolvedOptions(sys.argv, ['JOB_NAME','FULL_LOAD_FLAG','INCREMENTAL_DATE','ENVIRONMENT'])
jobName = args['JOB_NAME']
fullLoadFlag = args['FULL_LOAD_FLAG']
incrementalDate = args['INCREMENTAL_DATE']
env = args['ENVIRONMENT']
yamlName = jobName.rsplit('-', 1)[0]

###### Sync Variables ###### 
region_name = "us-east-1"
s3_bucket = 'random_bucket'
s3_file = f"random_bucket/random_location/{yamlName}.yaml"
getVariables = InitializeVariables()
table_list,beginTime = getVariables.Variables(region_name,s3_bucket,s3_file,jobName,incrementalDate)

###### Connection Details ######
connections = SyncConnections(region_name=region_name,connection_source='Oracle_dwprod01',connection_target='snowflake_dwprod')

###### Do the Load ###### 
for table in table_list:
    if table.get('load_type') == 'full' or fullLoadFlag == 'Y':
        tables = SyncTables(source_db = table.get('source_db')
                            ,source_schema = table.get('source_schema')
                            ,source_tname = table.get('source_tname')
                            ,target_db = table.get('target_db').replace("{{ENV}}",env)
                            ,target_schema = table.get('target_schema')
                            ,target_tname = table.get('target_tname')
                            ,cdc_column = table.get('cdc_column')
                            ,lookback_days = table.get('lookback_days')
                            ,primary_key = table.get('primary_key')
                            ,load_type = table.get('load_type')
                            ,beginTime = beginTime
                            )
        sync = GlueLoad()
        sync.FullLoad(connections,tables,glueContext)

    elif table.get('load_type') == 'append':
        tables = SyncTables(source_db = table.get('source_db')
                            ,source_schema = table.get('source_schema')
                            ,source_tname = table.get('source_tname')
                            ,target_db = table.get('target_db').replace("{{ENV}}",env)
                            ,target_schema = table.get('target_schema')
                            ,target_tname = table.get('target_tname')
                            ,cdc_column = table.get('cdc_column')
                            ,lookback_days = table.get('lookback_days')
                            ,primary_key = table.get('primary_key')
                            ,load_type = table.get('load_type')
                            ,beginTime = beginTime
                            )
        sync = GlueLoad()
        sync.IncrementalLoad(connections,tables,glueContext,spark)
```
