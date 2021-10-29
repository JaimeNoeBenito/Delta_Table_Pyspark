# Databricks notebook source
spark.sql ("""CREATE TABLE your_schema.your_table (     
Field_1 STRING, 
Field_2 STRING, 
Field_3 STRING,
Field_4 DATE
 )                                  
USING DELTA                      
LOCATION 'your_path_datalake'""")

# COMMAND ----------

%sql
CREATE TABLE your_schema.your_table ( 
Field_1 STRING, 
Field_2 STRING, 
Field_3 STRING,
Field_4 DATE) 
USING delta OPTIONS ( option 'snappy') 
LOCATION 'your_path_datalake';

# COMMAND ----------


