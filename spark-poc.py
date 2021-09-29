from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("tmnas troubleshooting").getOrCreate()

spark.conf.set("spark.yarn.access.hadoopFileSystems","abfs://data@smalikpoc.dfs.core.windows.net")

df = spark.read.json("abfs://data@smalikpoc.dfs.core.windows.net/tmnas/*.json")

df.show(10,True)

df.write.mode("overwrite").option("path","abfs://data@smalikpoc.dfs.core.windows.net/tmnas/hive/").saveAsTable("policy")

spark.sql("select * from policy").show(10,True)