# Databricks notebook source
# MAGIC %sql
# MAGIC show catalogs;

# COMMAND ----------

# MAGIC %sql
# MAGIC create catalog if not exists quickstart_catalog;

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog quickstart_catalog;

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists quickstart_catalog.quickstart_schema;

# COMMAND ----------

# MAGIC %sql
# MAGIC show schemas;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe schema extended quickstart_schema;

# COMMAND ----------

# MAGIC %sql
# MAGIC use quickstart_schema;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists quickstart_table
# MAGIC (
# MAGIC   column_a int, 
# MAGIC   column_b string
# MAGIC );
# MAGIC
# MAGIC insert into table quickstart_schema.quickstart_table values (1, 'one'), (2, 'two'), (3, 'three');

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog quickstart_catalog;
# MAGIC use schema quickstart_schema;
# MAGIC select * from quickstart_table;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from quickstart_catalog.quickstart_schema.quickstart_table;
