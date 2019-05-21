import sys
import uuid

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

source_data_bucket = sys.argv[1]
source_data = sys.argv[2]
intermediate_dir = sys.argv[3]

spark = SparkSession.builder.appName('simple-file-etl').master('yarn-client').config("spark.submit.deployMode",
                                                                                     "cluster").getOrCreate()
df = spark.read.format('com.databricks.spark.csv').option('header', 'true') \
    .option('delimiter', ',').load('gs://{}/{}'.format(source_data_bucket, source_data))

uuidUdf = udf(lambda: str(uuid.uuid4()), StringType())

newdf = df.withColumn('member_id', uuidUdf()).drop('id')

newdf.write.format('csv').option('header', 'true').option('escape', '\"').save(
    'gs://{}/{}'.format(source_data_bucket, intermediate_dir),
    mode='overwrite')
