// Databricks notebook source
displayHTML("Configuration running...")

// COMMAND ----------

//Configuration parameters

val stage = "debug"

// Set basePath according to your mount point
val basePath = "/mnt/datalake"

// Set basePath for the source streaming files
val sourcePath = "/mnt/blob"

// Data Lake configuration
val adlsStorageAccount = "lvstrgacct"
// The filesystem can be created in the portal or via the initialisation in cmd 6 in 0.Setup
val adlsFilesystem = "lv-mount-test"
// For the next two parameters follow https://docs.microsoft.com/en-gb/azure/active-directory/develop/howto-create-service-principal-portal#get-values-for-signing-in
// Directory ID - also referred to as tenant ID in Azure Active Directory
val adlsDirectoryID = "8c44bd9a-6f79-41fb-99e9-1a306676a3f5"
// Client ID - also referred to the application ID of the registered applicatin in Azure Active Directory
val adlsClientID = "46e3665a-4de2-4e30-81a7-cfa3594ad3d8" //"4e2a3951-f2ea-46db-b1cc-5f7c400bcf67"
val appEndpoint = "https://login.microsoftonline.com/"+adlsDirectoryID+"/oauth2/token"


// Override the default number of partitions created during shuffle
spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)

displayHTML("Configuration compete.")