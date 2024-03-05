# python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, concat, countDistinct, to_timestamp, year, count, month, to_date, \
    weekofyear
from pyspark.sql.types import StructType, ArrayType, StringType, StructField, IntegerType, BooleanType, FloatType

# Driver
spark = SparkSession \
    .builder \
    .master('local') \
    .appName('HelloSpark') \
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

result0 = df\
    .select("City")\
    .distinct().show()

#1 打印2018年份所有的CallType，并去重
result1 = df.filter(col("CallDate").like("%2018%"))\
    .select("CallType")\
    .distinct()
result1.show()

#2 2018年的哪个月份有最高的火警
result2 = df.filter(col("CallDate").like("%2018%"))\
    .filter(col("CallType").like("%Fire%"))\
    .withColumn("Month", month(to_date("CallDate","MM/dd/yyyy")))\
    .groupBy("Month").count()\
    .orderBy("count",ascending= False)\
    .first()
if result2 is not None:
    print("2018年火警最多的月份是{}月，共发生了{}次火警。".format(result2["Month"], result2["count"]))
else:
    print("2018年没有火警")

#3 San Francisco的哪个neighborhood在2018年发生的火灾次数最多？
result3 = df.filter(year(to_date("CallDate","MM/dd/yyyy")) == 2018)\
    .filter(col("CallType").like("%Fire%"))\
    .filter((col("City") == "SF") | (col("City") == "San Francisco"))\
    .groupBy("Neighborhood").count()\
    .orderBy("count", ascending=False)\
    .first()
if result3 is not None:
    print("2018年发生火灾次数最多的 San Francisco neighborhood 是：{}".format(result3["Neighborhood"]))
else:
    print("2018年未发生火灾")

#4 San Francisco的哪个neighborhood在2018年响应最慢？
result4 = df.filter(year(to_date("CallDate", "MM/dd/yyyy")) == 2018)\
    .filter(col("City") == "San Francisco")\
    .orderBy("Delay", ascending=False)\
    .select("Neighborhood", "Delay")\
    .first()
print("2018年响应最慢的 San Francisco neighborhood 是：{}，最大响应时间为：{}分钟".format(
    result4["Neighborhood"], result4["Delay"]))

#5 2018年的哪一周的火警次数最多
result5 = df.filter(year(to_date("CallDate", "MM/dd/yyyy")) == 2018)\
    .filter(col("CallType").like("%Fire%"))\
    .withColumn("Week", weekofyear(to_date("CallDate", "MM/dd/yyyy")))\
    .groupBy("Week").count()\
    .orderBy("count", ascending=False)\
    .first()
if result5 is not None:
    print("2018年火警次数最多的周是第{}周，共发生了{}次火警。".format(result5["Week"], result5["count"]))
else:
    print("2018年未发生火灾")

#6 数据集中任意值之间有关联（correlation）吗？
# 读取数据集
df = spark.read.option('header', True).schema(fire_schema).csv('dataset/sf-fire-calls.txt')
# 选择数值类型的列
numerical_columns = [col_name for col_name, col_type in df.dtypes if col_type in ["int", "float"]]
# 循环遍历每对列，计算相关性
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