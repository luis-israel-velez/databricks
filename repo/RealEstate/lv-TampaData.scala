// Databricks notebook source
print("Hello from Tampa!")

// COMMAND ----------

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// COMMAND ----------

val results1  = scala.io.Source.fromURL("https://arcgis.tampagov.net/arcgis/rest/services/OpenData/Location/MapServer/5/query?where=1%3D1&outFields=OBJECTID,FACILITYID,NAME,OWNER,OWNTYPE,FEATURECODE,FULLADDR,MUNICIPALITY,STATE,ZIPCODE,DESCRIPTION&outSR=4326&f=json").mkString

// COMMAND ----------

val dfResults = spark.read.json(Seq(results).toDS).select($"features.attributes" as "value")

// COMMAND ----------

display(dfResults.select($"value.FULLADDR"))

// COMMAND ----------

val results1  = scala.io.Source.fromURL("https://opendata.arcgis.com/datasets/351c1188773d4416b03f24bc2e85f1a7_9.geojson").mkString

// COMMAND ----------

val dfResults1 = spark.read.json(Seq(results1).toDS).select($"features.properties" as "value")

// COMMAND ----------

display(dfResults1.select($"value.CEMETERY"))