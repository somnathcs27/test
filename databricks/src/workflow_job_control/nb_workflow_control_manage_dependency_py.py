# Databricks notebook source
import time
from pyspark.sql.utils import AnalysisException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType
from datetime import datetime
from zoneinfo import ZoneInfo


# COMMAND ----------

# Retrieve all job-level parameters

dbutils.widgets.text("catalog", "")
catalog_name = dbutils.widgets.get("catalog")

dbutils.widgets.text("job_name", "")
job_name = dbutils.widgets.get("job_name")

dbutils.widgets.text("adls2_storage_account", "")
adls2_storage_account = dbutils.widgets.get("adls2_storage_account")

dbutils.widgets.text("error_detail", "")
error_detail = dbutils.widgets.get("error_detail")


# COMMAND ----------

def check_is_dependency () -> int:

    query = f"""
    SELECT (CASE WHEN EXISTS (SELECT 'x'
                              FROM {catalog_name}.workflow_control.job_detail a
                              INNER JOIN {catalog_name}.workflow_control.job_dependency b
                                ON a.pk_job_detail = b.fk_job_detail_dependency
                              WHERE a.job_name = '{job_name}')
                 THEN '1' ELSE '0' END) AS is_dependency"""

    df = spark.sql(query)

    return int(df.collect()[0]['is_dependency'])

# COMMAND ----------

def check_all_dependencies_met ():

    query = f"""
    SELECT 
        x.job_parent_name
        ,x.job_dependency_name
        ,MIN(x.dependencies_met) AS dependencies_met
    FROM 
    (
        SELECT 
            a.pk_job_detail,
            a.job_name as job_dependency_name,
            c.fk_job_detail_dependency,
            b.fk_job_detail,
            d.job_name as job_parent_name,
            (CASE WHEN d.status = 'SUCCESS' THEN 1 ELSE 0 END) AS dependencies_met
        FROM {catalog_name}.workflow_control.job_detail a
        INNER JOIN {catalog_name}.workflow_control.job_dependency b
        ON a.pk_job_detail = b.fk_job_detail_dependency
        INNER JOIN {catalog_name}.workflow_control.job_dependency c
        ON b.fk_job_detail = c.fk_job_detail
        INNER JOIN {catalog_name}.workflow_control.job_detail d
        ON b.fk_job_detail = d.pk_job_detail
        LEFT OUTER JOIN (
                            SELECT fk_job_detail, status
                            FROM {catalog_name}.workflow_control.completed_jobs
                            WHERE CAST(job_end_datetime AS DATE) = CAST(FROM_UTC_TIMESTAMP(NOW(), 'Europe/London') AS DATE)
                            QUALIFY ROW_NUMBER() OVER (PARTITION BY fk_job_detail ORDER BY job_end_datetime DESC) = 1
                        ) d
        ON c.fk_job_detail_dependency = d.fk_job_detail
        WHERE a.job_name = '{job_name}'
    ) x
    GROUP BY x.job_parent_name,x.job_dependency_name"""

    df = spark.sql(query)

    return df

# COMMAND ----------

def insert_completed_job ():
    
    query = f"""
    INSERT INTO {catalog_name}.workflow_control.completed_jobs ( fk_job_detail, job_name, job_end_datetime, status, error_detail, trigger_file_generated, trigger_file_path)
    SELECT pk_job_detail,
           job_name,
           FROM_UTC_TIMESTAMP(NOW(), 'Europe/London')      AS job_end_datetime,
           'SUCCESS'                                       AS status,
           ''                                              AS error_detail,
           0                                               AS trigger_file_generated,
           ''                                              AS trigger_file_path
    FROM {catalog_name}.workflow_control.job_detail
    WHERE job_name = '{job_name}'"""

    df = spark.sql(query)

# COMMAND ----------

def update_completed_job_with_trigger_file_path ( trigger_output_path:str ):
    
    query = f"""
    UPDATE {catalog_name}.workflow_control.completed_jobs
    SET trigger_file_generated = (CASE WHEN '{trigger_output_path}' = '' THEN 0 ELSE 1 END),
        trigger_file_path =  '{trigger_output_path}'
    WHERE job_name = '{job_name}'
    AND job_end_datetime = (SELECT MAX(job_end_datetime) FROM {catalog_name}.workflow_control.completed_jobs WHERE job_name = '{job_name}')
    """

    df = spark.sql(query)

# COMMAND ----------

def generate_trigger_file (row) -> str:

    spark = SparkSession.builder.appName("GenerateTriggerFile").getOrCreate()

    job_parent_name =  row["job_parent_name"]

    job_dependency_name =  row["job_dependency_name"]

  # now create trigger path...
    output_path = (
        "abfss://metadata@"
        + adls2_storage_account
        + ".dfs.core.windows.net/trigger_files/"
        + job_parent_name
        + "/"
        + job_dependency_name
        + "_"
        + datetime.now(ZoneInfo("Europe/London")).strftime('%Y%m%d%H%M')
        + ".txt"
    )
    
    dbutils.fs.put(output_path, job_parent_name, overwrite=True)

    return output_path

# COMMAND ----------

def post_error_record () -> str:

       consolidated_error_detail = ""

       # we we don't know which task triggered the error we need to gather all job task...
       query = f"""SELECT jt.task_key
                   FROM (SELECT job_id
                         FROM system.lakeflow.jobs
                         WHERE name = '{job_name}'
                         QUALIFY ROW_NUMBER() OVER (PARTITION BY name ORDER BY COALESCE(delete_time,CAST('9999-12-31' AS TIMESTAMP)) DESC, change_time DESC) = 1) j
                   INNER JOIN (SELECT job_id, task_key
                               FROM system.lakeflow.job_tasks
                               WHERE task_key LIKE '%load_gold%'
                               QUALIFY ROW_NUMBER() OVER (PARTITION BY job_id, task_key ORDER BY COALESCE(delete_time,CAST('9999-12-31' AS TIMESTAMP)) DESC, change_time DESC) = 1) jt
                      ON j.job_id = jt.job_id            
                    ORDER BY jt.task_key ASC"""

       df = spark.sql(query)

       #...then set a error message based on the population of each tasks error detail
       for tk in df.collect():
              task_error_detail = ""
              task_error_detail = dbutils.jobs.taskValues.get(taskKey=tk['task_key'], key="error_detail", default="")

              # break if we have found a populated error message
              if task_error_detail != "":
                     consolidated_error_detail = consolidated_error_detail + tk['task_key'] + ' failed: ' + task_error_detail + ', '

       #strip the closing delimiter
       consolidated_error_detail = consolidated_error_detail[:-2]

       # if none are set then we want to use the default job error message
       if consolidated_error_detail == "":
              consolidated_error_detail = error_detail

       #clean out single quotes for use within the dynamic SQL statement
       consolidated_error_detail = consolidated_error_detail.replace("'","")

       # finally want to popuate the completed jobs table with our consolidated variable... 
       query = f"""
       INSERT INTO {catalog_name}.workflow_control.completed_jobs ( fk_job_detail, job_name, job_end_datetime, status,    error_detail, trigger_file_generated, trigger_file_path)
       SELECT pk_job_detail,
              job_name,
              FROM_UTC_TIMESTAMP(NOW(), 'Europe/London')       AS job_end_datetime,
              'FAILED'                                         AS status,
              '{consolidated_error_detail}'                    AS error_detail,
              CAST(0 AS TINYINT)                               AS trigger_file_generated,
              ''                                               AS trigger_file_path
       FROM {catalog_name}.workflow_control.job_detail
       WHERE job_name = '{job_name}'"""

       df = spark.sql(query)

       return consolidated_error_detail

# COMMAND ----------

#if we're running this notebook in the context of a failure then post a failure and exit
if error_detail != "":
    consolidated_error_detail = post_error_record()
    dbutils.notebook.exit(consolidated_error_detail)

trigger_output_path = ''
trigger_output_paths = []

#Regardless of depency status post a job completed message
insert_completed_job()

#check if job acts as a dependency
is_dependency = check_is_dependency()

if is_dependency == 1:

    check_all_dependencies_met = check_all_dependencies_met()

    # Filter for dependencies that are met
    met_df = check_all_dependencies_met.filter(check_all_dependencies_met["dependencies_met"] == 1)

    # For each dependency met, run generate_trigger_file
    for row in met_df.collect():    

        path = generate_trigger_file(row)
        trigger_output_paths.append(path)

    trigger_output_path = "  |  ".join(trigger_output_paths)
    update_completed_job_with_trigger_file_path(trigger_output_path)
