import xml.etree.ElementTree as ElTree
from pyspark.sql import SparkSession
from pyspark.sql.functions import max, min, first, last, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import os
import sys

import time


schema = StructType([
    StructField('SYMBOL', StringType(), True),
    StructField('SYSTEM', StringType(), True),
    StructField('MOMENT', StringType(), True),
    StructField('ID_DEAL', IntegerType(), True),
    StructField('PRICE_DEAL', DoubleType(), True),
    StructField('VOLUME', IntegerType(), True),
    StructField('OPEN_POS', IntegerType(), True),
    StructField('DIRECTION', StringType(), True),
])


def config_getter(file_path='config.xml'):
    config = {
        'candle.width': '300000',
        'candle.date.from': '19000101',
        'candle.date.to': '20200101',
        'candle.time.from': '1000',
        'candle.time.to': '1800'
    }

    if os.path.isfile(file_path):
        tree = ElTree.parse(file_path)
        root = tree.getroot()

        for elem in root.findall('property'):
            name = elem.find('name').text
            value = elem.find('value').text
            if name in config:
                config[name] = value
    return dict(config)

def User_Round(num):
    return int(num * 10 + 0.5) / 10

candle_width = 0

def get_time_interval(time_str):
    hour, minute, second, msecond = time_str[8:10], time_str[10:12], time_str[12:14], time_str[14:]
    total = (int(hour) * 3600 + int(minute) * 60 + int(second)) * 1000 + int(msecond)
    rez = total // int(candle_width)
    return rez

def func(r):
    return r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], int(get_time_interval(r[2]))

def format(row):
    r0 = row[0]
    r2 = int(row[2])
    r3 = float(row[3])
    r4 = float(row[4])
    r5 = float(row[5])
    r6 = float(row[6])
    return r0, str(r2 - r2 % int(candle_width)), User_Round(r3), User_Round(r4), User_Round(r5), User_Round(r6)

if __name__ == '__main__':
    config_file_path = sys.argv[1] if len(sys.argv) > 1 else 'config.xml'
    print("Config parameters:")
    start = time.time()
    config = config_getter(config_file_path)
    for name, value in config.items():
        print(f"{name}: {value}")

    candle_width = config["candle.width"]

    cwd = os.getcwd()
    spark = SparkSession.builder \
        .appName('MyPySparkApp') \
        .config('spark.master', 'local[*]') \
        .config('spark.executor.cores', 8) \
        .config('spark.executor.memory', '6g') \
        .getOrCreate()

    data = spark.read.csv('input.csv', header=False, schema=schema)
    data = data.filter(col("SYMBOL") != "#SYMBOL")
    data.cache()

    names_column = data.select("SYMBOL")
    names_rdd = names_column.rdd.distinct()
    list_of_names = names_rdd.map(lambda x: x[0]).collect()

    folder_path = os.path.join(cwd, "result")
    if not os.path.exists(folder_path):
        os.mkdir(folder_path)

    data = data.filter((col("MOMENT").substr(1, 8) >= config["candle.date.from"]) &
                       (col("MOMENT").substr(1, 8) < config["candle.date.to"]) &
                       (col("MOMENT").substr(9, 4) >= config["candle.time.from"]) &
                       (col("MOMENT").substr(9, 4) < config["candle.time.to"]))

    rdd = data.rdd.map(func)
    df = spark.createDataFrame(rdd, data.columns + ["Interval"])
    df = df.groupBy(col("SYMBOL"), col("Interval")).agg(first("MOMENT").alias("MOMENT"), \
                                                        first("PRICE_DEAL").alias("OPEN"), \
                                                        max("PRICE_DEAL").alias("HIGH"),  \
                                                        min("PRICE_DEAL").alias("LOW"), \
                                                        last("PRICE_DEAL").alias("CLOSE"))

    rdd = df.rdd.map(format)
    df = spark.createDataFrame(rdd, ["SYMBOL", "MOMENT", "OPEN", "HIGH", "LOW", "CLOSE"]).cache()
    counter = len(list_of_names)
    for element in list_of_names:
        #counter -= 1
        #t = time.time()
        print(element)
        buf_df = df.filter(col("SYMBOL") == element)   #.distinct() оказывается произвольно меняет порядок строк
        file = buf_df.toPandas()
        file.to_csv(os.path.join(cwd, "result/" + str(element) + ".csv"), lineterminator = "\n", index = False, header = False)
        #print(f"{len(list_of_names) - counter}) time = %s " % (time.time() - t))
    print("Time = %s" % (time.time() - start))
    spark.stop()