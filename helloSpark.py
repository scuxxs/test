from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, concat, countDistinct, to_timestamp, year, count, to_date, month, \
    weekofyear
from pyspark.sql.types import StructType, ArrayType, StringType, StructField, IntegerType, BooleanType, FloatType

# Driver
spark = SparkSession \
    .builder \
    .master('local') \
    .appName('FireIncidentsAnalysis') \
    .getOrCreate()

fire_schema = StructType([StructField("CallNumber", IntegerType(), True),
                          StructField("UnitID", StringType(), True),
                          StructField("IncidentNumber", IntegerType(), True),
                          StructField("CallType", StringType(), True),
                          StructField("CallDate", StringType(), True),
                          StructField("WatchDate", StringType(), True),
                          StructField("CallFinalDisposition", StringType(), True),
                          StructField("AvailableDtTm", StringType(), True),
                          StructField("Address", StringType(), True),
                          StructField("City", StringType(), True),
                          StructField("Zipcode", IntegerType(), True),
                          StructField("Battalion", StringType(), True),
                          StructField("StationArea", StringType(), True),
                          StructField("Box", StringType(), True),
                          StructField("OriginalPriority", StringType(), True),
                          StructField("Priority", StringType(), True),
                          StructField("FinalPriority", IntegerType(), True),
                          StructField("ALSUnit", BooleanType(), True),
                          StructField("CallTypeGroup", StringType(), True),
                          StructField("NumAlarms", IntegerType(), True),
                          StructField("UnitType", StringType(), True),
                          StructField("UnitSequenceInCallDispatch", IntegerType(), True),
                          StructField("FirePreventionDistrict", StringType(), True),
                          StructField("SupervisorDistrict", StringType(), True),
                          StructField("Neighborhood", StringType(), True),
                          StructField("Location", StringType(), True),
                          StructField("RowID", StringType(), True),
                          StructField("Delay", FloatType(), True)
                          ]
                         )

df = spark.read.option('header', True).schema(fire_schema).csv('dataset/sf-fire-calls.txt')

# 处理年份
df = df.withColumn("CallDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
df = df.withColumn("Year", year("CallDate"))
df = df.withColumn("Month", month("CallDate"))
df = df.withColumn("Week", weekofyear(to_date("CallDate", "MM/dd/yyyy")))

# 1. 打印2018年份所有的CallType，并去重
callType_2018 = df.select("CallType").where(col("Year") == 2018).distinct().collect()
print("------------------------------------")
print("2018年份所有的CallType，并去重:")
for row in callType_2018:
    print(row[0])
# 2. 2018年的哪个月份有最高的火警
max_fire_counts_2018 = df.select("Month").where((col("Year") == 2018) & (col('CallType').like("%Fire%"))).groupby(
    "Month").agg(
    count("*").alias("FireCount")).orderBy(
    col("FireCount").desc())
print("------------------------------------")
print("2018年的哪个月份有最高的火警")
print(f"{max_fire_counts_2018.select('Month').first()[0]} 月有最高的火警")

# 3. San Francisco的哪个neighborhood在2018年发生的火灾次数最多？
sf_nei = df.select("Neighborhood").where(
    (col("Year") == 2018) & (col("city") == 'San Francisco') & (col('CallType').like("%Fire%"))).groupby(
    "Neighborhood").agg(
    count("*").alias("FireCount")).orderBy(col("FireCount").desc())
print("------------------------------------")
print("San Francisco的哪个neighborhood在2018年发生的火灾次数最多？")
if sf_nei.first() is not None:
    print(f"{sf_nei.select('Neighborhood').first()[0]}火灾最多")
else:
    print("没有")

# 4. San Francisco的哪个neighborhood在2018年响应最慢？
result4 = df.where((col('Year') == 2018) & (col('city') == 'San Francisco')).orderBy(col('Delay').desc()).select(
    'Neighborhood').first()[0]
print("------------------------------------")
print("San Francisco的哪个neighborhood在2018年响应最慢？")
print(f"{result4}响应最慢")

# 5.2018年的哪一周的火警次数最多

result5 = \
df.where((col('Year') == 2018) & (col('CallType').like("%Fire%"))).groupby("Week").agg(count("*").alias("c")).orderBy(
    col("c").desc()).first()[0]
print("------------------------------------")
print("2018年的哪一周的火警次数最多")
print(f"{result5}周次数最多")
# 6. 数据集中任意值之间有关联（correlation）吗？
df = spark.read.option('header', True).schema(fire_schema).csv('dataset/sf-fire-calls.txt')
# 选择数值类型的列
numerical_columns = [col_name for col_name, col_type in df.dtypes if col_type in ["int", "float"]]
# 循环遍历每对列，计算相关性
print("------------------------------------")
for i in range(len(numerical_columns)):
    for j in range(i + 1, len(numerical_columns)):
        col1, col2 = numerical_columns[i], numerical_columns[j]
        correlation = df.select(col(col1), col(col2)).stat.corr(col1, col2)
        print("列{}和列{}的相关性: {}".format(col1, col2, correlation))

#7 实现使用parquest存储并读取
# 保存为Parquet文件
df.write.mode("overwrite").parquet("output/weekly_fire_calls.parquet")
# 从Parquet文件读取数据
loaded_df = spark.read.parquet("output/weekly_fire_calls.parquet")
# 打印结果
loaded_df.show(truncate=False)

df.where(col('CallType').isNotNull())\
    .groupby('CallType') \
    .agg(count('*').alias('count')) \
    .orderBy(col('count').desc()).show(truncate=False)