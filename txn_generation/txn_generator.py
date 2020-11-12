import findspark
findspark.init()
import sys
import random
from operator import add

from pyspark.sql import SparkSession#, SQLContext
from pyspark.sql.types import StructType, StructField, StringType
import time

num_steps = 365 #number of days in simulation
random_numbers = [random.random() for x in range(num_steps)]



if __name__ == "__main__":
    """
        Usage: pi [partitions]
    """
    spark = SparkSession\
        .builder\
        .appName("txnGenerator")\
        .getOrCreate()

    start = time.time()
    '''txn_data_schema = StructType([StructField("type", StringType(), True),
     StructField("account_from", StringType(), True)])'''
    #sc = SQLContext(spark)

    df = spark.read.csv('../link_generation/links_df.csv', inferSchema=True, header=True)
    '''df = sc.read.load("../link_generation/links_df.csv",
                    header='true',
                    inferSchema='true').cache()'''

    '''partitions = int(sys.argv[1]) if len(sys.argv) > 1 else 2
    n = 100000 * partitions

    def f(_):
        x = random() * 2 - 1
        y = random() * 2 - 1
        return 1 if x ** 2 + y ** 2 <= 1 else 0

    count = spark.sparkContext.parallelize(range(1, n + 1), partitions).map(f).reduce(add)
    print("Pi is roughly %f" % (4.0 * count / n)) # count/n is the number of points x,y that lie outside of a circle radius 1'''
    print(type(df))
    print(type(df.select("txn_prob")))
    print(type(df.select(["txn_prob","amount"]).collect()[0]))
    for row in df.select("txn_prob").collect():
      if row["txn_prob"] > 0.1:
        pass
        #print("Transact!")
    print("Spark job takes {:.2f}s".format(time.time() - start))

    spark.stop()
