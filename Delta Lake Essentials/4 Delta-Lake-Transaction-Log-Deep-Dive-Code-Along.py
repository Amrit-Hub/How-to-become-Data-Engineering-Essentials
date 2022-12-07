# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Partner Solution Architect Essentials
# MAGIC ## Delta Lake Transaction Log Deep Dive
# MAGIC 
# MAGIC Delta Lake’s transaction log brings high reliability, performance, and ACID compliant transactions to data lakes. But exactly how does it accomplish this?
# MAGIC Working through concrete examples, we will take a close look at how the transaction logs are used by Delta Lake to provide high reliability and performance.
# MAGIC 
# MAGIC ### Topics covered
# MAGIC * Creating Delta tables
# MAGIC * Viewing table metadata and table versioning via the "history" command
# MAGIC * How Delta manages the log files
# MAGIC * What goes into the transaction logs for various DML operations
# MAGIC * How Delta constructs snapshots of data
# MAGIC * The small file problem and how to mitigate it
# MAGIC * How to construct time travel queries
# MAGIC * Configuring Delta tables for deleted files and log retention
# MAGIC 
# MAGIC ### Additional resources
# MAGIC * Accompanying video and OSS version of this notebook can be found in Github at <a href="https://github.com/databricks/tech-talks/tree/master/2020-08-27%20%7C%20How%20Delta%20Lake%20Supercharges%20Data%20Lakes" target="_blank">databricks/tech-talks.</a>
# MAGIC 
# MAGIC * The data used in this tutorial is a modified version of the public data from <a href="http://archive.ics.uci.edu/ml/datasets/Online+Retail#" target="_blank">UCI Machine Learning Repository</a>. This dataset contains transactional data from a UK online retailer and it spans January 12, 2010 to September 12, 2011. For a full view of the data please view the data dictionary available <a href="http://archive.ics.uci.edu/ml/datasets/Online+Retail#" target="_blank">here</a>.
# MAGIC 
# MAGIC ### Requirements
# MAGIC * DBR 8.0 or higher

# COMMAND ----------

# MAGIC %md
# MAGIC <br>
# MAGIC <img src="https://docs.delta.io/latest/_static/delta-lake-logo.png" width=300/>  
# MAGIC 
# MAGIC An open-source storage format that brings ACID transactions to Apache Spark™ and big data workloads.
# MAGIC 
# MAGIC * **Open format**: Stored as Parquet format in blob storage.
# MAGIC * **ACID Transactions**: Ensures data integrity and read consistency with complex, concurrent data pipelines.
# MAGIC * **Schema Enforcement and Evolution**: Ensures data cleanliness by blocking writes with unexpected.
# MAGIC * **Audit History**: History of all the operations that happened in the table.
# MAGIC * **Time Travel**: Query previous versions of the table by time or version number.
# MAGIC * **Deletes and upserts**: Supports deleting and upserting into tables with programmatic APIs.
# MAGIC * **Scalable Metadata management**: Able to handle millions of files are scaling the metadata operations with Spark.
# MAGIC * **Unified Batch and Streaming Source and Sink**: A table in Delta Lake is both a batch table, as well as a streaming source and sink. Streaming data ingest, batch historic backfill, and interactive queries all just work out of the box.
# MAGIC 
# MAGIC <img src="https://www.evernote.com/l/AAF4VIILJtFNZLuvZjGGhZTr2H6Z0wh6rOYB/image.png" width=800px align="center">

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup and Configurations

# COMMAND ----------

# Required Classes
from pyspark.sql.functions import sum, expr, rand, when, count, col
# from delta.tables import * # Main class for programmatically intereacting with Delta Tables via Python. e.g path based tables

# Set configurations for our Spark Session
spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Run the following cells to configure our notebook and load helpers.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %run ./Helpers

# COMMAND ----------

# MAGIC %md
# MAGIC # Define Data File Paths

# COMMAND ----------

sourceData   = "/mnt/training/online_retail/data-001/"

# Base location for all saved data
basePath = userhome 

# Path for Parquet formatted data
parquetPath  = basePath + "/parquet/online_retail_data"

# Path for Delta formatted data
deltaPath    = basePath + "/delta/online_retail_data"
deltaLogPath = deltaPath + "/_delta_log"

# Clean up from last run.
dbutils.fs.rm(deltaPath, True)
print("Deleted path " + deltaPath)

dbutils.fs.rm(parquetPath, True)
print("Deleted path " + parquetPath)

# COMMAND ----------

# MAGIC %md
# MAGIC # Define Schema for Source Data

# COMMAND ----------

# Provide schema for source data
# Schema source: http://archive.ics.uci.edu/ml/datasets/Online+Retail#
schemaDDL = """InvoiceNo Integer, StockCode String, Description String, Quantity Integer, 
               InvoiceDate String, UnitPrice Double, CustomerID Integer, Country String """


# COMMAND ----------

# MAGIC %md
# MAGIC # Explore and Clean Data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a DataFrame from CSV
# MAGIC 
# MAGIC For this exercise, use a file called **data.csv** and have a peek at the data.

# COMMAND ----------

# Create retail sales data dataframe from csv

rawSalesDataDF = (
    spark.read
    .format("csv")
    .option("header","true")
    .schema(schemaDDL)
    .load(sourceData)
)

# Count rows
rowCount = rawSalesDataDF.count() 

print(f'Row Count: {rowCount}')

# COMMAND ----------

[count(when(col(c).isNull(), c)).alias(c) for c in rawSalesDataDF.columns]
  

# COMMAND ----------

# Identify columns with null values

print("Columns with null values")
rawSalesDataDF.select([count(when(col(c).isNull(), c)).alias(c) for c in rawSalesDataDF.columns]).show()

# COMMAND ----------

# Remove rows where important columns are null. In our case: InvoiceNo and CustomerID

cleanSalesDataDF = rawSalesDataDF.where(col("InvoiceNo").isNotNull() & col("CustomerID").isNotNull())
cleanSalesDataCount = cleanSalesDataDF.count()
# POO cleanSalesDataDF = cleanSalesDataDF.where(col("CustomerID").isNotNull())

# All rows with null values should be gone
print("null values")
cleanSalesDataDF.select([count(when(col(c).isNull(), c)).alias(c) for c in rawSalesDataDF.columns]).show()

print(f' RowsRemoved: {rowCount-cleanSalesDataCount}\n Final Row Count: {cleanSalesDataCount}')

# COMMAND ----------

# Define new dataframe based on a subset of the cleansed data.
# We will use this dataframe a few cells down.

# Random sample of 25%, with seed and without replacement
retailSalesData1 = cleanSalesDataDF.sample(withReplacement=False, fraction=.25, seed=75)

# Count rows
rowCount = retailSalesData1.count()

print(f'Row Count: {rowCount}')

# COMMAND ----------

# MAGIC %md
# MAGIC # HIVE Metastore Database Setup

# COMMAND ----------

# Create database for demo objects
db_name = spark.conf.get("com.databricks.training.spark.databaseName")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")

# Current DB should be deltademo
spark.sql(f"USE {db_name}")
spark.sql("SELECT CURRENT_DATABASE()").show(truncate = False)
spark.sql(f"DESCRIBE DATABASE {db_name}").show(truncate = False)

# Clean-up from last run
spark.sql("DROP TABLE IF EXISTS SalesParquetFormat")
spark.sql("DROP TABLE IF EXISTS SalesDeltaFormat")
spark.sql("DROP TABLE IF EXISTS tbl_CheckpointFile")
spark.sql("SHOW TABLES").show()


# COMMAND ----------

# MAGIC %md
# MAGIC # Working with Parquet Files

# COMMAND ----------

# Save data as a table in Parquet format

retailSalesData1.write.saveAsTable('SalesParquetFormat', format='parquet', mode='overwrite',path=parquetPath)

# Let's peek into the catalog and verify that our table was created
spark.sql("show tables").show(truncate=False)

# COMMAND ----------

# Files and size on disk
display(dbutils.fs.ls(parquetPath))

# COMMAND ----------

# MAGIC %sql DESCRIBE TABLE EXTENDED SalesParquetFormat;

# COMMAND ----------

# Use Spark SQL to query the newly created table

spark.sql("SELECT * FROM SalesParquetFormat;").show(5, truncate = False)

# You can directly query the directory too.
# spark.sql(f"SELECT * FROM parquet.`{parquetPath}` limit 5 ").show(truncate = False)

# COMMAND ----------

# Add one row of data to the table
# Parquet being immutable necessitates the creation of an additional Parquet file

spark.sql(""" 
             INSERT INTO SalesParquetFormat
              VALUES(963316, 2291, "WORLD'S BEST JAM MAKING SET", 5, "08/13/2011 07:58", 1.45, 15358, "United Kingdom")
          """)

# COMMAND ----------

# Files and size on disk
display(dbutils.fs.ls(parquetPath))

# COMMAND ----------

# Try updating a Parquet file format table

spark.sql(""" 
             UPDATE SalesParquetFormat
             SET Quantity = 5
             WHERE InvoiceNo = 536365
          """)

# COMMAND ----------

# MAGIC %md
# MAGIC - Above we saw how to create a table structure using Parquet as the underlying data file format.<br>
# MAGIC - Parquet file format allows for append operations. e.g. Table inserts.<br>
# MAGIC - Parquet file format does not allow for DML operations. e.g Update, Delete, Merge.
# MAGIC - Parquet file format does not keep history.<br>

# COMMAND ----------

# MAGIC %md
# MAGIC # Working with Delta Lake

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://files.training.databricks.com/images/adbcore/AAFxQkg_SzRC06GvVeatDBnNbDL7wUUgCg4B.png" alt="Delta Lake" width="600" align="left"/>

# COMMAND ----------

# Save parquet format sales data as delta format.
dfParquet = spark.read.format("parquet").load(parquetPath)
dfParquet.write.mode("overwrite").format("delta").save(deltaPath)

# Query the Delta directory directly
display(spark.sql(f"SELECT * FROM delta.`{deltaPath}` limit 5 "))

# COMMAND ----------

# Create Delta table on top of the existing Delta directory.

spark.sql("""
    DROP TABLE IF EXISTS SalesDeltaFormat
  """)
spark.sql("""
    CREATE TABLE SalesDeltaFormat
    USING DELTA
    LOCATION '{}'
  """.format(deltaPath))

# COMMAND ----------

# MAGIC %sql DESCRIBE HISTORY SalesDeltaFormat

# COMMAND ----------

# Delta files and size on disk. Notice they are identical to the Parquet files above.
# Notice the sub-directory "_delta_log"

display(dbutils.fs.ls(deltaPath))

# COMMAND ----------

# We can see that parquet files were added along with a transactions log.

display(dbutils.fs.ls(deltaLogPath))

# COMMAND ----------

# Peek inside the checksum (crc) file.
# The checksum ensures integrity of the log file.
# If a checkpoint ever became corrupt (for any reason) it will be ignored.

# histogramOpt:        A Histogram class tracking the file counts and total bytes in different size ranges
# fileCounts:          # an array of Int representing total number of files in different bins
# sortedBinBoundaries: an array of Long representing total number of bytes in different bins
#                      sortedBinBoundries = a sorted list of bin boundaries where each element represents 
#                      the start of the bin (included) and the next element represents the end of the bin (excluded)
# totalBytes:          an array of Long representing total number of bytes in different bins

display(spark.read.format("json").load(deltaLogPath + "/00000000000000000000.crc"))

# COMMAND ----------

# History schema details: https://docs.delta.io/latest/delta-utility.html

# Peek inside the transaction log.
# What meta-data can you discern?
# Be warned, it is messy!

display(spark.read.format("json").load(deltaLogPath + "/00000000000000000000.json"))

# COMMAND ----------

# Create a new dataframe with fraction of original data (cleanSalesDataDF).
# Random sample of 25%, with seed and without replacement

retailSalesData2 = cleanSalesDataDF.sample(withReplacement=False, fraction=.25, seed=31)
retailSalesData2.count()

# COMMAND ----------

# Add to the Delta table by appending retailSalesData2.

retailSalesData2.write.mode("append").format("delta").save(deltaPath)

# COMMAND ----------

# MAGIC %sql DESCRIBE HISTORY SalesDeltaFormat

# COMMAND ----------

# Data Files and size on disk
# Our append operation resulted in new Parquet files. Verify by looking at version 1 of the table, see above cell.

display(dbutils.fs.ls(deltaPath))

# COMMAND ----------

# Transaction logs and size on disk
# Notice the new transaction log "1.json", it contains information about the append operation above.

display(dbutils.fs.ls(deltaLogPath))

# COMMAND ----------

# Peek inside the new transaction log
# What meta-data can you discern?

display(spark.read.format("json").load(deltaLogPath + "/00000000000000000001.json"))
#logDF.collect()

# COMMAND ----------

# MAGIC %sql DESCRIBE EXTENDED SalesDeltaFormat

# COMMAND ----------

# Let's find a Invoice with only 1 line item and use it to test DML.

oneRandomInvoice = spark.sql(""" SELECT InvoiceNo, count(*)
                                 FROM SalesDeltaFormat
                                 GROUP BY InvoiceNo
                                 ORDER BY 2 asc
                                 LIMIT 1
                             """).collect()[0][0]

print(f"Random Invoice # => {oneRandomInvoice}")

# Only 1 line item for this invoice.
spark.sql(f"""
              SELECT SUBSTRING(input_file_name(), -67, 67) AS FileName,
                     * FROM SalesDeltaFormat 
              WHERE InvoiceNo = {oneRandomInvoice}
           """).show(truncate = False)

# COMMAND ----------

# Add 1 line item to the table.

spark.sql(f"""
               INSERT INTO SalesDeltaFormat
               VALUES({oneRandomInvoice}, 2291, "WORLD'S BEST JAM MAKING SET", 5, "08/13/2011 07:58", 1.45, 15358, "France");
          """)

# COMMAND ----------

# There is now another transaction log: 2.json.
display(dbutils.fs.ls(deltaLogPath))

# COMMAND ----------

# Peek inside the new transaction log
# What meta-data can you discern?

display(spark.read.format("json").load(deltaLogPath + "/00000000000000000002.json"))

# COMMAND ----------

# Query the Delta table to see newly added line item.
# Notice that a new Parquet file was added. 
# Parquet files are immutable so any changes results in a new Parquet file.

spark.sql(f"""
              SELECT SUBSTRING(input_file_name(), -67, 67) AS FileName, *
                     FROM SalesDeltaFormat 
                     WHERE InvoiceNo = {oneRandomInvoice}
           """).show(truncate = False)

# COMMAND ----------

# Perform a random update to the invoice.
# Add 1000 to Quantity.

spark.sql(f"""
              UPDATE SalesDeltaFormat
              SET Quantity = Quantity + 1000
              WHERE InvoiceNo = {oneRandomInvoice}
           """)


# COMMAND ----------

# After the update.

spark.sql(f"""
              SELECT 
              SUBSTRING(input_file_name(), -67, 67) AS FileName, *
              FROM SalesDeltaFormat 
              WHERE InvoiceNo = {oneRandomInvoice}
           """).show(truncate = False)

# COMMAND ----------

# MAGIC %sql
# MAGIC /* Notice that version 3 "operationParameters" includes the predicate from the update.*/
# MAGIC DESCRIBE HISTORY SalesDeltaFormat

# COMMAND ----------

# Two new files were added as a result of this update.
# Do you know why there are two new files when we only updated two records?

display(dbutils.fs.ls(deltaPath))

# COMMAND ----------

# Before DML (delete)

spark.sql(f"""select 
          substring(input_file_name(), -67, 67) as FileName,
          * from SalesDeltaFormat 
          where InvoiceNo = {oneRandomInvoice}""").show(truncate = False)

# COMMAND ----------

# Delete an invoice (two records)

# This results in one new file being created.  One file had just the one record so it does not have to be re-created
# Each of the two records were in two different files. One of those files had only one record so it did not have to be re-created.

spark.sql(f"DELETE FROM SalesDeltaFormat WHERE InvoiceNo = {oneRandomInvoice}")


# COMMAND ----------

# After DML (delete)

spark.sql(f"""
              SELECT 
              SUBSTRING(input_file_name(), -67, 67) as FileName, *
              FROM SalesDeltaFormat 
              WHERE InvoiceNo = {oneRandomInvoice}
          """).show(truncate = False)

# COMMAND ----------

# MAGIC %sql
# MAGIC /* Notice that two files were deleted but only 1 was added */
# MAGIC DESCRIBE HISTORY SalesDeltaFormat

# COMMAND ----------

# Remember, when file are "removed" they are not physically removed, they have a tombstone marker placed on them.
# Notice below that we have a net gain of 1 file after the delete operaion.

display(dbutils.fs.ls(deltaPath))

# COMMAND ----------


display(dbutils.fs.ls(deltaLogPath))

# COMMAND ----------

# Look at the meta-data, you can see the two files that were removed "remove=Row(.....)" and one file was added.
display(spark.read.format("json").load(deltaLogPath + "/00000000000000000004.json"))

# COMMAND ----------

# Randomy update several random invoices to force a checkpoint. i.e 10.json => 10.checkpoint.parquet

count = 0
anInvoice = retailSalesData2.select("InvoiceNo").orderBy(rand()).limit(1).collect()[0][0]

while (count <= 7):
  spark.sql(f"UPDATE SalesDeltaFormat set Quantity = Quantity + 100 WHERE InvoiceNo = {anInvoice}")
  
  count = count + 1
  anInvoice = retailSalesData2.select("InvoiceNo").orderBy(rand()).limit(1).collect()[0][0]

# COMMAND ----------


display(dbutils.fs.ls(deltaLogPath))

# COMMAND ----------

checkPointDF = spark.read.format("parquet").load(deltaLogPath + "/00000000000000000010.checkpoint.parquet")
display(checkPointDF)

# COMMAND ----------

# Organize all the file deletes and additions into one structure.

checkPointFile10 =(
    checkPointDF.select(col("add.path").alias("FileAdded"),
                        col("add.modificationTime").alias("DateAdded"),
                        col("remove.path").alias("FileDeleted"),
                        col("remove.deletionTimestamp").alias("DateDeleted"))
                .orderBy(["DateAdded","DateDeleted"], ascending=[True,False])
)

spark.sql("DROP TABLE IF EXISTS tbl_checkpointfile")
spark.sql("CREATE TABLE IF NOT EXISTS tbl_checkpointfile (Action string, filename string, ActionDate Long)")

# Create temporay view so that we can use SQL
checkPointFile10.createOrReplaceTempView("vw_checkpointfile")

# Add operations to tbl_checkpointfile. This will allow us to organize the additions and deletes chronologically.

spark.sql("""
          INSERT INTO tbl_checkpointfile
          SELECT "Add", FileAdded, DateAdded
          FROM vw_checkpointfile
          WHERE FileAdded IS NOT NULL
          """)

spark.sql("""
          INSERT INTO tbl_checkpointfile
          SELECT "Delete", FileDeleted, DateDeleted
          FROM vw_checkpointfile
          WHERE FileDeleted IS NOT NULL
          """)

# Easy to read chronology of files added and deleted.*
# This only contains those transactions up to and including 10.json.

spark.sql("""
           SELECT Action, 
                  filename as `File Name`, 
                  from_unixtime(actiondate/1e3) AS `ActionDate`
           FROM tbl_checkpointfile 
           order by ActionDate asc
          """).show(200, truncate=False)

# Notice that there are only 4 active (Add) files. These files represent the current state of the Delta table.

spark.sql("""
          SELECT DISTINCT(input_file_name())
          FroM SalesDeltaFormat
          """)

# COMMAND ----------

# Let's add some data to our table using MERGE (upsert).

# Create a tiny dataframe to use with merge
mergeSalesData= cleanSalesDataDF.sample(withReplacement=False, fraction=.0001, seed=13)
mergeSalesData.createOrReplaceTempView("vw_mergeSalesData")

# User-defined commit info metadata. Just showing that this is an option but not required.
spark.sql(f"""
               SET spark.databricks.delta.commitInfo.userMetadata=08-25-2020 Data Merge;
          """)

spark.sql("""
          MERGE INTO SalesDeltaFormat
          USING vw_mergeSalesData
          ON SalesDeltaFormat.StockCode = vw_mergeSalesData.StockCode
           AND SalesDeltaFormat.InvoiceNo = vw_mergeSalesData.InvoiceNo
          WHEN MATCHED THEN
            UPDATE SET *
          WHEN NOT MATCHED
            THEN INSERT *
          """)

# Reset user-defined commit info.
spark.sql(f"""
               SET spark.databricks.delta.commitInfo.userMetadata=" ";
          """)

# COMMAND ----------

# MAGIC %sql DESCRIBE HISTORY SalesDeltaFormat

# COMMAND ----------

display(dbutils.fs.ls(deltaLogPath))

# COMMAND ----------

# Count the number of Parquet files in deltaPath.

file_count(deltaPath)

# COMMAND ----------

# MAGIC %md
# MAGIC # Delta Lake File Compaction

# COMMAND ----------

# Create an artificial "small file" problem
# Take our small Delta table and force it to be distributed across 500 Parquet files.

(spark.read
.format("delta")
.load(deltaPath)
.repartition(500)
.write
.option("dataChange", True)
.format("delta")
.mode("overwrite")
.save(deltaPath)
)


# COMMAND ----------

# Count files in deltaPath

file_count(deltaPath)

# COMMAND ----------

# MAGIC %%time 
# MAGIC 
# MAGIC # This simple count will take more time than is needed because spark has to open over 500 files.
# MAGIC 
# MAGIC rowCount = spark.sql(""" SELECT CustomerID, count(Country) AS num_countries
# MAGIC                          FROM SalesDeltaFormat
# MAGIC                          GROUP BY CustomerID 
# MAGIC                      """).count()
# MAGIC 
# MAGIC print(f"Row Count => {rowCount}\n")

# COMMAND ----------

# MAGIC %sql
# MAGIC /* Run Delta's optimize process to reduce the number of datafiles */
# MAGIC OPTIMIZE SalesDeltaFormat

# COMMAND ----------

# Count files in deltaPath
# How many files do you think will be present after the optimize process?
# Is the result what you expected?

file_count(deltaPath)

# COMMAND ----------

# MAGIC %%time 
# MAGIC 
# MAGIC # Let's run that summary query again and see if the compaction process helps with performance.
# MAGIC 
# MAGIC rowCount = spark.sql(""" select CustomerID, count(Country) as num_countries
# MAGIC                          from SalesDeltaFormat
# MAGIC                         group by CustomerID """).count()
# MAGIC 
# MAGIC print(f"Row Count => {rowCount}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Delta Time Travel Queries

# COMMAND ----------

# Time Travel Queries

# Determine latest version of the Delta table
currentVersion = spark.sql("DESCRIBE HISTORY SalesDeltaFormat LIMIT 1").collect()[0][0]

# Query table as of the current version to attain row count
currentRowCount = spark.read.format("delta").option("versionAsOf", currentVersion).load(deltaPath).count()

print(f"Row Count: {currentRowCount} as of table version {currentVersion}")
print("")

# COMMAND ----------

# MAGIC %sql DESCRIBE HISTORY SalesDeltaFormat

# COMMAND ----------

# Determine difference in record count between the current version and the original version of the table.

origRowCount = spark.read.format("delta").option("versionAsOf", 0).load(deltaPath).count()
print(f"There are {currentRowCount-origRowCount} more rows in version [{currentVersion}] than version [0] of the table.")

# COMMAND ----------

# MAGIC %sql -- Roll back current table to version 0 (original).
# MAGIC RESTORE SalesDeltaFormat VERSION AS OF 0

# COMMAND ----------

# Current version should have same record count as version 0.

currentVersion = spark.sql("DESCRIBE HISTORY SalesDeltaFormat LIMIT 1").collect()[0][0]

# Returns "true" if equal
spark.read.format("delta").option("versionAsOf", currentVersion).load(deltaPath).count() == spark.read.format("delta").option("versionAsOf", 0).load(deltaPath).count()

# COMMAND ----------

# MAGIC %sql DESCRIBE HISTORY SalesDeltaFormat

# COMMAND ----------

# MAGIC %md
# MAGIC # Delta Vacuum - Data Retention

# COMMAND ----------

# MAGIC %md
# MAGIC delta.logRetentionDuration - default 30 days
# MAGIC <br>
# MAGIC delta.deletedFileRetentionDuration - default 30 days
# MAGIC 
# MAGIC * Don't need to set them to be the same.  You may want to keep the log files around after the tombstoned files are purged.
# MAGIC * Time travel in order of months/years infeasible
# MAGIC * Initially desinged to correct mistakes

# COMMAND ----------

# Count files in deltaPath

file_count(deltaPath)

# COMMAND ----------

# Attempt to vacuum table (dry run) with default settings.
# This will produce an error!

spark.sql("VACUUM SalesDeltaFormat RETAIN 0 HOURS DRY RUN")

# COMMAND ----------

# Disable guardrail

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", False)

# COMMAND ----------

# Vacuum Delta table to remove all history

display(spark.sql("VACUUM SalesDeltaFormat RETAIN 0 HOURS"))

# COMMAND ----------

# Show that most of the parquet files are now gone.
display(dbutils.fs.ls(deltaPath))

# COMMAND ----------

# MAGIC %sql DESCRIBE HISTORY SalesDeltaFormat

# COMMAND ----------

spark.read.format("delta").option("versionAsOf", currentVersion).load(deltaPath).count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data retention
# MAGIC 
# MAGIC To time travel to a previous version, you must retain both the log and the data files for that version. <br>
# MAGIC 
# MAGIC The data files backing a Delta table are never deleted automatically; **data files are deleted only when you run VACUUM**. <br>
# MAGIC VACUUM does not delete Delta log files; log files are automatically cleaned up after checkpoints are written.
# MAGIC 
# MAGIC By default you can time travel to a Delta table up to 30 days old unless you have:
# MAGIC 
# MAGIC * Run VACUUM on your Delta table.
# MAGIC 
# MAGIC * Changed the data or log file retention periods using the following table properties:
# MAGIC 
# MAGIC   * delta.logRetentionDuration = "interval <interval>": controls how long the history for a table is kept. The default is interval 30 days.
# MAGIC 
# MAGIC         Each time a checkpoint is written, Databricks automatically cleans up log entries older than the retention interval. 
# MAGIC         If you set this config to a large enough value, many log entries are retained. This should not impact performance as operations 
# MAGIC         against the log are constant time. Operations on history are parallel but will become more expensive as the log size increases.
# MAGIC 
# MAGIC   * delta.deletedFileRetentionDuration = "interval <interval>": controls how long ago a file must have been deleted before being a candidate for VACUUM. The default is interval 7 days.
# MAGIC   
# MAGIC         To access 30 days of historical data even if you run VACUUM on the Delta table, set delta.deletedFileRetentionDuration = "interval 30 days". This setting may cause your storage costs to go up.
# MAGIC   
# MAGIC   
# MAGIC From the documentation: https://docs.databricks.com/delta/delta-utility.html <br>
# MAGIC From the documentation: https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-vacuum.html

# COMMAND ----------

# Configure Delta table to keep around 7 days of deleted data and 7 days of older log files
spark.sql("alter table SalesDeltaFormat set tblproperties ('delta.logRetentionDuration' = 'interval 7 days', 'delta.deletedFileRetentionDuration' = 'interval 7 days')")

# Verify our changes
spark.sql("describe extended SalesDeltaFormat").show(100, truncate = False)
