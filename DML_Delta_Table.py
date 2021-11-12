# DBTITLE 1, ADD column 
%sql
ALTER TABLE your_schema.your_table ADD columns (your_column_name_String STRING )
ALTER TABLE your_schema.your_table ADD columns (your_column_name_Date DATE)

# DBTITLE 1,Rename column 
df = spark.sql("Select * from your_schema.your_table")

df1 = df.withColumnRenamed("your_column_OLD_Name", "your_column_NEW_Name")

spark.sql("ALTER TABLE your_schema.your_table RENAME TO your_schema.your_table_OLD ") #Copy table for backup - it can be removed later

df1.write.format("delta").mode("OVERWRITE").option("overwriteSchema", "true").saveAsTable("your_schema.your_table")
