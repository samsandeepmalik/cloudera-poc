git clone https://github.com/samsandeepmalik/cloudera-poc.git

cd cloudera-poc

spark-submit --master yarn --deploy-mode cluster spark-poc.py

hdfs dfs -ls abfs://data@smalikpoc.dfs.core.windows.net/tmnas/hive/*