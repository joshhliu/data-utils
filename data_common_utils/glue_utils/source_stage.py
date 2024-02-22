"""
A module for running glue staging jobs.
"""
import boto3
import json
import yaml
import gs_now
from awsglue import DynamicFrame

class InitializeVariables:
    def Variables(self,region_name,s3_bucket,s3_file,jobName,incrementalDate):

        s3 = boto3.client('s3')
        gl = boto3.client('glue', region_name=region_name)

        # Table list from YAMLs
        response = s3.get_object(Bucket=s3_bucket, Key=s3_file)
        yaml_content = response['Body'].read().decode('utf-8')
        yaml_content = yaml.safe_load(yaml_content)
        table_list = yaml_content.get('tables', [])

        #Date Params from Glue Logs
        glueJobRuns = gl.get_job_runs(JobName=jobName)
        glueJobRunsSuccess = [(x["JobRunState"], x["StartedOn"].strftime('%Y-%m-%d %H:%M:%S')) for x in glueJobRuns["JobRuns"] if x["JobRunState"]=="SUCCEEDED"]

        if incrementalDate != "N":
            beginTime = incrementalDate #from glue params override begin date
        elif len(glueJobRunsSuccess) == 0:
            beginTime = '2024-01-01 00:00:00'
        else:
            beginTime = max(glueJobRunsSuccess, key=lambda x: x[1])[1]

        return(table_list,beginTime)


class SyncConnections:
    def __init__(self,region_name,connection_source,connection_target):
        connection_source_list = [
            {"connection_type_source": "oracle", "connection_source": "Oracle_dwprod01","source_secret":"glue-dwprod01-jdbc"},
            {"connection_type_source": "oracle", "connection_source": "EBS_DYEPRD1","source_secret":"glue-ebs-jdbc"},
            {"connection_type_source": "oracle", "connection_source": "data-svc-prodwind","source_secret":"glue-plm-jdbc"},
            {"connection_type_source": "oracle", "connection_source": "noetixods_read","source_secret":"glue-edwpr01-jdbc"},
            {"connection_type_source": "mysql", "connection_source": "kwi_usa_read","source_secret":"glue-kwi-us-jdbc"},
            {"connection_type_source": "mysql", "connection_source": "kwi_ca_read","source_secret":"glue-kwi-ca-jdbc2"},
            {"connection_type_source": "mysql", "connection_source": "kwi_hk_read","source_secret":"glue-kwi-hk-jdbc2"},
            {"connection_type_source": "mysql", "connection_source": "kwi_eu_read","source_secret":"glue-kwi-eu-jdbc2"},
            {"connection_type_source": "mysql", "connection_source": "kwi_uk_read","source_secret":"glue-kwi-uk-jdbc2"},
            {"connection_type_source": "mysql", "connection_source": "bosslogics_dy_data_v2","source_secret":""},
            {"connection_type_source": "mysql", "connection_source": "bosslogics__dy_wholesale","source_secret":"glue-boss-dy-wholesale-jdbc"},
            {"connection_type_source": "mysql", "connection_source": "bosslogics__boss_together_dy","source_secret":""},
        ]

        connection_target_list = [
            {"connection_type_target": "snowflake", "connection_target": "snowflake_dwprod","target_secret":"glue-dwprod01-jdbc"},
        ]

        for connection in connection_source_list:
            if connection["connection_source"] == connection_source:
                connection_type_source = connection["connection_type_source"]
                source_secret = connection["source_secret"]

        for connection in connection_target_list:
            if connection["connection_target"] == connection_target:
                connection_type_target = connection["connection_type_target"]
                #target_secret = connection["target_secret"]

        # Secrets for Incremental Load - JDBC
        sm = boto3.client('secretsmanager', region_name=region_name)
        source_get_secret_value_response = sm.get_secret_value(SecretId=source_secret)
        source_secret = source_get_secret_value_response['SecretString']
        source_secret = json.loads(source_secret)

        self.connection_source = connection_source
        self.connection_type_source = connection_type_source
        self.connection_target = connection_target
        self.connection_type_target = connection_type_target
        self.source_secret = source_secret

class SyncTables:
    def __init__(self,source_db,source_schema,source_tname,target_db,target_schema,target_tname,cdc_column,lookback_days,primary_key,beginTime,load_type):
        self.source_db = source_db
        self.source_schema = source_schema
        self.source_tname = source_tname
        self.target_db = target_db
        self.target_schema = target_schema
        self.target_tname = target_tname
        self.cdc_column = cdc_column
        self.lookback_days = lookback_days
        self.primary_key = primary_key
        self.beginTime = beginTime
        self.load_type = load_type

class GlueLoad():
    """
    Class for staging source tables through AWS Glue (Truncate Load)
    """
    def FullLoad(self,connections,tables,glueContext):
        print('source: ', tables.source_db+'.'+tables.source_schema+'.'+tables.source_tname, tables.load_type)
        print('target: ', tables.target_db+'.'+tables.target_schema+'.'+tables.target_tname, tables.load_type)

        source_create_df =glueContext.create_dynamic_frame.from_options(
            connection_type=connections.connection_type_source,
            connection_options={
                "useConnectionProperties": "true",
                "dbtable": f"{tables.source_schema}.{tables.source_tname}",
                "connectionName": connections.connection_source,
            }
        )
        
        source_df_time = source_create_df.gs_now(colName="_LOAD_TIMESTAMP")
        
        print('total source row count: ',source_df_time.count())
        
        target_write_dynamic_frame = glueContext.write_dynamic_frame.from_options(
            frame=source_df_time,
            connection_type=connections.connection_type_target,
            connection_options={
                "autopushdown": "on",
                "dbtable": tables.target_tname,
                "connectionName": connections.connection_target,
                "preactions": f"TRUNCATE TABLE {tables.target_db}.{tables.target_schema}.{tables.target_tname}",
                "sfDatabase": tables.target_db,
                "sfSchema": tables.target_schema,
            },
        )
        print('Complete: ', tables.target_db+'.'+tables.target_schema+'.'+tables.target_tname)

    def IncrementalLoad(self,connections,tables,glueContext,spark):
        print('source: ', tables.source_db+'.'+tables.source_schema+'.'+tables.source_tname, tables.load_type)
        print('target: ', tables.target_db+'.'+tables.target_schema+'.'+tables.target_tname, tables.load_type)

        if tables.lookback_days:
            if connections.connection_type_source == 'mysql':
                sql_query = f"SELECT * FROM {tables.source_schema}.{tables.source_tname} a WHERE {tables.cdc_column} >= CURDATE()-{tables.lookback_days}"
            elif connections.connection_type_source == 'oracle':
                sql_query = f"SELECT * FROM {tables.source_schema}.{tables.source_tname} a WHERE {tables.cdc_column} >= TRUNC(CURRENT_DATE)-{tables.lookback_days}"
            else: 
                sql_query = ''
        else:
            if connections.connection_type_source == 'mysql':
                sql_query = f"SELECT * FROM {tables.source_schema}.{tables.source_tname} a WHERE {tables.cdc_column} >= STR_TO_DATE('{beginTime}','YYYY-MM-DD HH24:MI:SS')"
            elif connections.connection_type_source == 'oracle':
                sql_query = f"SELECT * FROM {tables.source_schema}.{tables.source_tname} a WHERE {tables.cdc_column} >= cast('{beginTime}','YYYY-MM-DD HH24:MI:SS')" #CONVERT TIMEZONES IF NEEDED
            else: 
                sql_query =''

        print('sql_query: ',sql_query)
        
        source_df = (spark.read.format("jdbc")
            .option("url", connections.source_secret.get('jdbc'))
            .option("user", connections.source_secret.get('username'))
            .option("password", connections.source_secret.get('password'))
            .option("query", sql_query)
            .load()
            )

        source_df_time = source_df.gs_now(colName="_LOAD_TIMESTAMP")
        convert_to_dynamic_frame = DynamicFrame.fromDF(source_df_time, glueContext, "convert_to_dynamic_frame")        
        
        print('source row count: ',convert_to_dynamic_frame.count())

        if tables.primary_key == None:
            preaction_query = ""
            postaction_query = ""
            target_tname_tmp = tables.target_tname
        else:
            target_tname_tmp = tables.target_tname+"_tmp"
            primary_key = tables.primary_key.split(',')
            target_join = f"USING {target_tname_tmp} b WHERE"
            
            for key in primary_key:
                if primary_key.index(key) == 0:
                    target_join += f" a.{key} = b.{key}"
                else:
                    target_join += f" AND a.{key} = b.{key}"

            preaction_query = f"CREATE TABLE IF NOT EXISTS {target_tname_tmp} LIKE {tables.target_tname};"
            postaction_query = f"DELETE FROM {tables.target_tname} a {target_join};\
                                INSERT INTO {tables.target_tname} (SELECT * FROM {target_tname_tmp});\
                                DROP TABLE {target_tname_tmp};"
        
        print('preaction_query: ', preaction_query)
        print('postaction_query: ', postaction_query)
        
        target_write_dynamic_frame = glueContext.write_dynamic_frame.from_options(
            frame=convert_to_dynamic_frame,
            connection_type=connections.connection_type_target,
            connection_options={
                "autopushdown": "on",
                "dbtable": target_tname_tmp,
                "connectionName": connections.connection_target,
                "preactions": preaction_query,
                "postactions": postaction_query,
                "sfDatabase": tables.target_db,
                "sfSchema": tables.target_schema,
            },
        )

        print('Complete: ', tables.target_db+'.'+tables.target_schema+'.'+tables.target_tname)