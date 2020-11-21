import findspark
findspark.init()
import sys
import random
from operator import add

from pyspark.sql import SparkSession#, SQLContext
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from pyspark.sql.functions import udf, explode
import time

num_steps = 2 #number of days in simulation


def transact(txn_prob, txn_type, account_from, account_to, MoP, amount):
    random_numbers = [random.random() for x in range(num_steps)]
    return [[timestep, txn_type, account_from, account_to, MoP, amount] for timestep, x in enumerate(random_numbers) if x < txn_prob]


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
    # then use the df.select('col_name') function to return the txn column as a
    # new dataframe
    transaction_df = df.withColumn("transactions", udf_transact(df["txn_prob"], \
                                   df["type"], df["account_from"], \
                                   df["account_to"], df["MoP"], df["amount"])) \
                                   .select("transactions")
    transaction_explode_df = transaction_df.select(explode(transaction_df["transactions"]) \
                                                   .alias("transactions"))

    list_length = len(transaction_explode_df.select('transactions').take(1)[0][0])
    transaction_explode_df.show()

    '''transaction_output_df = transaction_explode_df \
        .select([transaction_explode_df["transactions"][i] for i in range(list_length)])

    transaction_output_df.show()'''

    print("Spark job takes {:.2f}s".format(time.time() - start))

    spark.stop()
