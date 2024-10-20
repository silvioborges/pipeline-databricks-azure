# Databricks notebook source
dbutils.fs.ls("/mnt/dados/bronze")

# COMMAND ----------

# MAGIC %scala
# MAGIC val path = "dbfs:/mnt/dados/bronze/dataset_imoveis/"
# MAGIC val df = spark.read.format("delta").load(path)

# COMMAND ----------

# MAGIC %scala  
# MAGIC display(df)

# COMMAND ----------

# MAGIC %scala
# MAGIC display(df.select("anuncio.*"))

# COMMAND ----------

# MAGIC %scala
# MAGIC display(
# MAGIC   df.select("anuncio.*", "anuncio.endereco.*")
# MAGIC )

# COMMAND ----------

# MAGIC %scala
# MAGIC val dados_detalhados = df.select("anuncio.*", "anuncio.endereco.*")

# COMMAND ----------

# MAGIC %scala
# MAGIC val df_silver = dados_detalhados.drop("caracteristicas", "endereco")
# MAGIC display(df_silver)

# COMMAND ----------

# MAGIC %scala
# MAGIC val path = "dbfs:/mnt/dados/silver/dataset_imoveis"
# MAGIC df_silver.write.format("delta").mode("overwrite").save(path)
# MAGIC
