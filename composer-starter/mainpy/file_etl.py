import uuid

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName('simple-file-etl').master('yarn-client').config("spark.submit.deployMode",
                                                                                     "cluster").getOrCreate()
df = spark.read.format('com.databricks.spark.csv').option('header', 'true') \
    .option('delimiter', ',').load('gs://src_raw-data_bucket/loan.csv')

uuidUdf = udf(lambda: str(uuid.uuid4()), StringType())

newdf = df.withColumn('member_id', uuidUdf()).drop('id')

newdf.write.format('csv').option('header', 'true').option('escape', '\"').save('gs://src_raw-data_bucket/loan-1',
                                                                               mode='overwrite')
