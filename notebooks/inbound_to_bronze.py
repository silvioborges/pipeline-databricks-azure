# Databricks notebook source
dbutils.fs.ls("/mnt/dados/inbound")

# COMMAND ----------

# MAGIC %scala
# MAGIC val path = "dbfs:/mnt/dados/inbound/dados_brutos_imoveis.json"
# MAGIC val dados = spark.read.json(path)

# COMMAND ----------

# MAGIC %scala
# MAGIC display(dados)

# COMMAND ----------

# MAGIC %scala
# MAGIC val dados_anuncio = dados.drop("imagens", "usuario")
# MAGIC display(dados_anuncio)

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.functions.col
# MAGIC
# MAGIC val df_bronze = dados_anuncio.withColumn("id", col("anuncio.id"))
# MAGIC display(df_bronze)

# COMMAND ----------

# MAGIC %scala
# MAGIC val path = "dbfs:/mnt/dados/bronze/dataset_imoveis"
# MAGIC df_bronze.write.format("delta").mode(SaveMode.Overwrite).save(path)
