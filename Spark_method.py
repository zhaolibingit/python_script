# -*- coding: utf-8 -*-
# @Time    : 2018/9/6 11:48
# @Author  : zhaolibin
# @Email   : 13021999163@163.com
# @File    : Spark_method.py
# @Software: PyCharm


##spark session
import os,pickle,sys
from pyspark.sql import SparkSession

SPARK_HOME = '/data/bigdata/spark-2.0.1-bin-hadoop2.7'
spark_url = 'spark://master:7077'
hdfs_url = 'hdfs://192.168.50.240:9000/tmp/ag_test.csv'
################################################################################

spark = SparkSession.builder.master(spark_url).appName("SPARK APP").getOrCreate()

################################################################################
#读取csv文件，header= True 代表第一行为列名

data = spark.read.csv(hdfs_url,header=True)

################################################################################
#列选择

colums_name = ['date','open']
data.select(colums_name)

################################################################################
#空缺值填充
fill_value = '999'
fill_colums = ['date','open']
data.fillna(value=fill_value,subset=fill_colums)



###############################################################################
#数据最大、最小、平均等分布情况
colums_name = ['date','open']
data.describe(colums_name)

################################################################################
#类型转换
from pyspark.sql.types import FloatType
colums_name = ['date','open','high']
data2 = data.select(colums_name)
data2 = data2.withColumn('open',data2['open'].cast(FloatType()))

################################################################################
#修改列名
colum = 'high'
new_colum = 'new_high'
data2.withColumnRenamed(colum,new_colum).show()

################################################################################
#添加新列
from pyspark.sql import functions
new_colum_name = 'new_colum_name'
values = functions.lit(777)
data2.withColumn(new_colum_name,values).show()

################################################################################
























