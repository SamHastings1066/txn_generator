import findspark
findspark.init()
import sys
import random
from operator import add

from pyspark.sql import SparkSession#, SQLContext
from pyspark.sql.types import StringType, ArrayType, IntegerType, DoubleType, StructType, StructField
from pyspark.sql.functions import udf, explode, col
import time

num_steps = 365 #number of days in simulation


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
    udf_transact = udf(transact, ArrayType(ArrayType(StringType())))

    '''StructType([
    StructField("timestep", IntegerType(), nullable = True),
    StructField("txn_type", StringType(), nullable = True),
    StructField("account_from", StringType(), nullable = True),
    StructField("account_to", StringType(), nullable = True),
    StructField("MoP", StringType(), nullable = True),
    StructField("amount", IntegerType(), nullable = True)]))))'''
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
    #transaction_explode_df.show()
    '''Have a look at the first value in txn_eplxode_df, see what type it is  -
    the error message looks like it might be a string. if it's a list see how you
    refer to the elemtns of the list, if it's a string need to split on comma'''
    #print(type(transaction_explode_df.head()[0]))
    #print(transaction_explode_df.head()[0])

    transaction_output_df = transaction_explode_df \
        .select([transaction_explode_df["transactions"][i] for i in range(list_length)])

    oldColumns = transaction_output_df.schema.names
    newColumns = ["timestep", "txn_type", "account_from", "account_to", "MoP", "amount"]

    names_as = [(oldColumns[idx], name) for idx, name in enumerate(newColumns)]

    for name in names_as:
        transaction_output_df = transaction_output_df.withColumnRenamed(name[0],name[1])

    transaction_output_df.show()
    print(transaction_output_df.count())

    print("Spark job takes {:.2f}s".format(time.time() - start))

    spark.stop()
