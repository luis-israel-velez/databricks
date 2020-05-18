// Databricks notebook source
//The following notebook is to explore the newly mounted (Storage Account)

val df = spark.read.format("delta").load("/mnt/datalake/adverts")

df.filter(df.col("brand") === "Nokia").select($"*")

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select * from delta.`/mnt/datalake/adverts` where  brand = "Nokia"