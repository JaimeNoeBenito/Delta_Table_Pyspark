# Databricks notebook source
# MAGIC %run import_your_functions_notebook

# COMMAND ----------

#Create widget for parameters

# dbutils.widgets.text("_path", "")
# dbutils.widgets.text("_filename", "")

_path = dbutils.widgets.get("_path")
_filename = dbutils.widgets.get("_filename")

# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import *
from datetime import datetime
import pandas as pd

table_Staging_Name = "your_tablename_history" #Name of the source table in Staging

# COMMAND ----------

# Create DELTA table
deltaTable = DeltaTable.forName(spark,'staging.' + table_Staging_Name)

# COMMAND ----------

# display(deltaTable.toDF())

# COMMAND ----------



# COMMAND ----------

# string columns
string_columns = {'string_field_1':str,
                  'string_field_2':str,
                  'string_field_3':str}

# Read new field (Pandas) - 
file_pd = pd.read_csv(pd_file, sep=';', dtype=string_columns, encoding='latin-1')

# COMMAND ----------

#Addition of new columns (3 --> Current, Start_Date and End_date
file_pd.insert(7,"field_current",True,True)
file_pd.insert(8,"field_endDate","null",True)
file_pd.insert(9,"field_insertDate",datetime.now(),True)




# COMMAND ----------

# Add field to update the file name 
file_pd.insert(10,"source_filename",_name,True)

# COMMAND ----------

#Date column and date format
date_columns = ['date_field_1','date_field_2', 'date_field_3'] 
date_format = '%Y-%m-%d'

for date_column in date_columns:
  file_pd[date_column] = pd.to_datetime(file_pd[date_column], format=date_format, errors='coerce') #Coerce errors
  

# COMMAND ----------

# Renaming the columns - Create alias if necessary
column_rename = {'field_1':'alias_alias_field_1',
                 'field_2':'alias_alias_field_2',
                 'field_3':'alias_alias_field_3',
                 'field_startDate':'stg_StartDate',
                 'field_current':'stg_current',
                 'field_endDate':'stg_dataEndDate',
                 'field_insertDate':'stg_insertDate',
                 'field_fileName':'stg_fileName'}

file_pd.rename(columns=column_rename,inplace=True)


# COMMAND ----------

# Spark Schema
schema = StructType([StructField("string_field_1", StringType(), True),
                     StructField("string_field_2", StringType(), True),
                     StructField("string_field_3", StringType(), True),
                     
                     StructField("date_field_1", DateType(), True),
                     StructField("date_field_2", DateType(), True),
                     StructField("date_field_3", DateType(), True),
                     
                     StructField("stg_StartDate", DateType(), True),
                     StructField("stg_current", BooleanType(), True),
                     StructField("stg_EndDate", DateType(), True),
                     StructField("stg_insertDate", TimestampType(), True),
                     StructField("stg_fileName", StringType(), True)])


# COMMAND ----------

#Converting Pandas DF into an Spark DF
spark_df = spark.createDataFrame(file_pd, schema = schema)

# COMMAND ----------

# Rows to INSERT 
newRecordsToInsert = spark_df \
  .alias("file") \
  .join(deltaTable.toDF().alias("table"), ["PK_field"],how='inner') \
  .where('''table.stg_current = true 
            AND (
              NOT(file.string_field_1 <=> table.string_field_1) 
              OR NOT(file.string_field_2 <=> table.string_field_2) 
              OR NOT(file.date_field_1 <=> table.date_field_1) 
              OR NOT(file.date_field_2  <=> table.date_field_2) 
              OR NOT(file.date_field_3 <=> table.date_field_3)
            )''')

# COMMAND ----------

# STAGING:
# Update the 2 sets 
# 1 - Rows that will be inserted in the whenNotMatched clause
# 2 - Rows that will either update or inserted
stagingUpdates = (
  newRecordsToInsert
  .selectExpr("NULL as mergeKey",
              "file.string_field_1",
              "file.string_field_2",
              "file.date_field_1",
              "file.date_field_2",
              "file.date_field_3"
              "file.stg_current",
              "file.stg_StartDate",
              "file.stg_EndDate",
              "file.stg_fileName",
              "file.stg_insertDate")   # Rows fors set 1
  .union(spark_df.selectExpr("PK_field as mergeKey",
                             "string_field_1",
                             "string_field_2",
                             "date_field_1",
                             "date_field_2",
                             "date_field_3",
                             "stg_current",
                             "stg_StartDate",
                             "stg_EndDate",
                             "stg_fileName",
                             "stg_insertDate"))  # Rows for set 2
)


# COMMAND ----------

# Apply SCD Type 2 operation using merge

deltaTable.alias("table").merge(
  stagingUpdates.alias("file"),
  "table.PK_field = mergeKey") \
.whenMatchedUpdate(
  condition = """table.stg_current = true 
                AND (
                  NOT(file.string_field_1 <=> table.string_field_1) 
                  OR NOT(file.string_field_2 <=> table.string_field_2) 
                  OR NOT(file.date_field_1 <=> table.date_field_1) 
                  OR NOT(file.date_field_2  <=> table.date_field_2) 
                  OR NOT(file.date_field_3 <=> table.date_field_3)
                )""",
  set = {                                      
    "stg_current": "false", # Set current to false
    "stg_EndDate": "file.stg_StartDate" #The end date of one is the previous of the one
  }
).whenNotMatchedInsert(
  values = {
    "string_field_1": "file.string_field_1",
    "string_field_2": "file.string_field_2",
    "date_field_1": "file.date_field_1",  
    "date_field_2": "file.date_field_2",
    "date_field_3": "file.date_field_3",
    "stg_current" : "true", # Set current to true along with the new field updates
    "stg_StartDate" : "file.stg_StartDate",
    "stg_EndDate" : "null",
    "stg_insertDate" : "file.stg_insertDate",
    "stg_fileName" : "file.stg_fileName"
  }
).execute()

# COMMAND ----------

#move the file so it will not be processed again
dbutils.fs.mv("input_dir", "archive_dir")
