from pyspark.shell import spark
from pyspark.sql import Window
from pyspark.sql.functions import col, dense_rank
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, concat, countDistinct, to_timestamp, year, count, month, to_date, \
    weekofyear
from pyspark.sql.types import StructType, ArrayType, StringType, StructField, IntegerType, BooleanType, FloatType

spark = SparkSession \
    .builder \
    .master('local') \
    .appName('window') \
    .getOrCreate()
dataset = [
    ("Thin", "Cell phone", 6000),
    ("Normal", "Tablet", 1500),
    ("Mini", "Tablet", 5500),
    ("Ultra thin", "Cell phone", 5000),
    ("Very thin", "Cell phone", 6000),
    ("Big", "Tablet", 2500),
    ("Bendable", "Cell phone", 3000),
    ("Foldable", "Cell phone", 3000),
    ("Pro", "Tablet", 4500),
    ("Pro2", "Tablet", 6500)
]
df = spark.createDataFrame(dataset, ['product','category','revenue'])
