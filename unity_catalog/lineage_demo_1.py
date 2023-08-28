# Databricks notebook source
# MAGIC %sql
# MAGIC create catalog if not exists lineage_catalog;
# MAGIC create schema if not exists lineage_catalog.lineage_schema;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists
# MAGIC   lineage_catalog.lineage_schema.menu (
# MAGIC     recipe_id int,
# MAGIC     app string,
# MAGIC     main string,
# MAGIC     dessert string
# MAGIC   );
# MAGIC
# MAGIC   insert into lineage_catalog.lineage_schema.menu 
# MAGIC     (recipe_id, app, main, dessert)
# MAGIC     values
# MAGIC     (1, 'Ceviche', 'Tacos', 'Flan'),
# MAGIC     (2, 'Tomato soup', 'Souffle', 'Creme Brulee'),
# MAGIC     (3, 'Chips', 'Grilled Cheese', 'Cheesecake');
# MAGIC
# MAGIC create table 
# MAGIC   lineage_catalog.lineage_schema.dinner
# MAGIC as select
# MAGIC   recipe_id, concat(app, ' + ', main, ' + ', dessert)
# MAGIC as full_menu
# MAGIC from lineage_catalog.lineage_schema.menu;

# COMMAND ----------

from pyspark.sql.functions import rand, round
df = spark.range(3).withColumn('price', round(10*rand(seed=42), 2)).withColumnRenamed('id', 'recipe_id')

df.write.mode('overwrite').saveAsTable('lineage_catalog.lineage_schema.price')

dinner = spark.read.table('lineage_catalog.lineage_schema.dinner')
price = spark.read.table('lineage_catalog.lineage_schema.price')

dinner_price = dinner.join(price, on = 'recipe_id')
dinner_price.write.mode('overwrite').saveAsTable('lineage_catalog.lineage_schema.dinner_price')

# COMMAND ----------


