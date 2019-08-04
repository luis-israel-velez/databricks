// Databricks notebook source
// MAGIC %md ######0.0 Run configuration script

// COMMAND ----------

// MAGIC %run ./Configuration

// COMMAND ----------

// MAGIC %md #####Storage account options
// MAGIC Depending whether you are using Blob storage  or Data Lake gen2, run the appropriate cells below.
// MAGIC Note if you are running in minimal setup mode, i.e. streaming from files then at least mount the sourcePath with adverts and impressions folders below.

// COMMAND ----------

// MAGIC %md #####1. Mount blob storage

// COMMAND ----------

//uncomment and run this to unmount
//dbutils.fs.unmount(basePath)

// COMMAND ----------

//dbutils.fs.mount(
//  source = "wasbs://"+blobContainer+"@"+blobAccount+".blob.core.windows.net",
//  mountPoint = basePath,
//  extraConfigs =  Map("fs.azure.account.key."+blobAccount+".blob.core.windows.net" -> dbutils.secrets.get(scope = "dwdemo", key = "blobstorage")))


// COMMAND ----------

// Create this additional blob mount point if you are streaming from files, i.e. runnning the minimal setup using notebook 2b
// Ensure the files are 
//dbutils.fs.mount(
//  source = "wasbs://"+blobContainer+"@"+blobAccount+".blob.core.windows.net",
//  mountPoint = sourcePath,
//  extraConfigs =  Map("fs.azure.account.key."+blobAccount+".blob.core.windows.net" -> dbutils.secrets.get(scope = "dwdemo", key = "blobstorage")))


// COMMAND ----------

// MAGIC %md #####2. Data lake - initialise a filesystem
// MAGIC This is only necessary if you haven't created a filesystem in your storage account already. Please see the <a href="https://docs.databricks.com/spark/latest/data-sources/azure/azure-datalake-gen2.html#azure-data-lake-storage-gen2">documentation</a> for more details

// COMMAND ----------

// configure the account name below. e.g. fs.azure.account.key.<account_name>.dfs.core.windows.net
spark.conf.set("fs.azure.account.key."+adlsStorageAccount+".dfs.core.windows.net",  dbutils.secrets.get(scope = "databricksKV", key = "datalake"))
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")
//configure below e.g abfss://<file_system>@<account_name>.dfs.core.windows.ne
dbutils.fs.ls("abfss://"+adlsFilesystem+"@"+adlsStorageAccount+".dfs.core.windows.net/")
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "false")

// COMMAND ----------

// MAGIC %md #####3. Data lake - mount filesystem
// MAGIC Readme: Creating a mount point with OAuth 2.0 requires service principal with delegated permissions, please see the <a href="https://docs.databricks.com/spark/latest/data-sources/azure/azure-datalake-gen2.html#azure-data-lake-storage-gen2">documentation</a> for steps required
// MAGIC <br>Update the parameters below according to your set up

// COMMAND ----------

val configs = Map(
  "fs.azure.account.auth.type" -> "OAuth",
  "fs.azure.account.oauth.provider.type" -> "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id" -> adlsClientID,
  "fs.azure.account.oauth2.client.secret" -> dbutils.secrets.get(scope = "databricksKV", key = "databrickssp"),
  "fs.azure.account.oauth2.client.endpoint" -> appEndpoint)

// Optionally, you can append <your-directory-name> at the end of the URI. e.g datalake@acmedwdemo.dfs.core.windows.net/<your-directory-name
dbutils.fs.mount(
  source = "abfss://"+adlsFilesystem+"@"+adlsStorageAccount+".dfs.core.windows.net/",
  mountPoint = basePath,
  extraConfigs = configs)


// COMMAND ----------

// uncomment and run this command to unmount
// dbutils.fs.unmount("/mnt/datalake/")

// COMMAND ----------

// MAGIC %md #####4. Optimise no. of Shuffles
// MAGIC By default Spark executes a shuffle with 200 partitions, it should be set according to the number of cores and volume of data

// COMMAND ----------

spark.conf.get("spark.sql.shuffle.partitions")

// COMMAND ----------

print(sc.defaultParallelism)

// COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)