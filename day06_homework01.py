from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, weekofyear, count, when, to_date

# 创建 SparkSession
spark = SparkSession.builder \
    .appName("FireIncidentsAnalysis") \
    .getOrCreate()
# 读取数据
df = spark.read.option("delimiter", ",").csv(".\dataset\sf-fire-calls.txt", header=True, inferSchema=True)
# 添加年份列
df = df.withColumn("CallDate", to_date("CallDate", "MM/dd/yyyy"))  # 将 CallDate 字符串转换为日期格式
df = df.withColumn("Year", year("CallDate"))
df = df.withColumn("Month", month("CallDate"))

# 1.打印2018年份所有的CallType，并去重
print('*' * 50)
print('2018年份所有的CallType')
df.filter('year = 2018').select('CallType').dropDuplicates().show()
print('*' * 50)

# 2.2018年的哪个月份有最高的火警
print('*' * 50)
print('2018年最高的火警次数的月份')
most_fires_month_2018 = df.filter('year = 2018')\
    .groupby('Month')\
    .agg(count("*").alias("FireCount"))\
    .orderBy(col("FireCount").desc())\
    .select('Month').first()[0]
print(most_fires_month_2018)
print('*' * 50)

# 3.San Francisco的哪个neighborhood在2018年发生的火灾次数最多？
print('*' * 50)
print('San Francisco在2018年发生的火灾次数最多的neighborhood')
sf_neighborhood_most_fires_row = df.filter((df.Year == 2018) & (df.City == 'SAN FRANCISCO')).\
    groupby("Neighborhood").\
    agg(count("*").alias("FireCount")).\
    orderBy(col("FireCount").desc()).\
    select("Neighborhood").first()
if sf_neighborhood_most_fires_row is not None:
    sf_neighborhood_most_fires = sf_neighborhood_most_fires_row[0]
    print(sf_neighborhood_most_fires)
else:
    print("没有")
print('*' * 50)

# 4.San Francisco的哪个neighborhood在2018年响应最慢？
print('*' * 50)
print('San Francisco在2018年响应最慢的neighborhood')
sf_neighborhood_slowest_row = df.filter((df.Year == 2018) & (df.City == "SAN FRANCISCO")).\
    groupby("Neighborhood").\
    agg(count("*").alias("FireCount")).\
    orderBy(col("FireCount").desc(), "Neighborhood").\
    select("Neighborhood").first()
if sf_neighborhood_slowest_row is not None:
    sf_neighborhood_slowest = sf_neighborhood_slowest_row[0]
    print(sf_neighborhood_slowest)
else:
    print("没有")
print('*' * 50)

# 5.2018年的哪一周的火警次数最多
print('*' * 50)
print('2018年火警次数最多的周')
week_most_fires_2018_row = df.filter(df.Year == 2018).\
    groupby(weekofyear("CallDate").alias("Week")).\
    agg(count("*").alias("FireCount")).\
    orderBy(col("FireCount").desc()).\
    select("Week").first()
if week_most_fires_2018_row is not None:
    week_most_fires_2018 = week_most_fires_2018_row[0]
    print(week_most_fires_2018)
else:
    print("没有火灾")
print('*' * 50)

# 6.数据集中任意值之间有关联（correlation）吗？
print('*' * 50)
print('数据集中NumAlarms与Delay之间的相关性')
correlation_matrix = df.corr('NumAlarms', 'Delay')
print(correlation_matrix)
print('*' * 50)

# 7.实现使用parquet存储并读取
print('*' * 50)
# 使用parquet存储数据
df.write.parquet("test.parquet")
# 读取parquet数据
parquet_df = spark.read.parquet("test.parquet")
parquet_df.show()
print('*' * 50)
