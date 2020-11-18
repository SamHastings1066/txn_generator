import findspark
findspark.init()
import sys
import random
from operator import add

from pyspark.sql import SparkSession#, SQLContext
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from pyspark.sql.functions import udf
import time

num_steps = 2 #number of days in simulation


def transact(txn_prob, amount):
    random_numbers = [random.random() for x in range(num_steps)]
    return [amount for x in random_numbers if x < txn_prob]


if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("txnGenerator")\
        .getOrCreate()

    start = time.time()

    #sc = SQLContext(spark)

    df = spark.read.csv('../link_generation/links_df.csv', inferSchema=True, header=True)

    #print(type(df))
    #print(type(df.select("txn_prob")))
    # convert transact() to a spark sql udf, explicitly setting its return type
    # to be a list of strings ArrayType(StringType())
    udf_transact = udf(transact, ArrayType(StringType()))
    # add a new column "transactions" to df, the values of this column will be
    # the result of applying udf_transact elementwise to the txn_prob column.
    # N.B. inputs to sql udfs MUST BE columns, e.g. cannot be intergers
    df.withColumn("transactions", udf_transact(df["txn_prob"], df["amount"])).show()




    print("Spark job takes {:.2f}s".format(time.time() - start))

    spark.stop()
