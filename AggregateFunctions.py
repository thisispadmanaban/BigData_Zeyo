import pyspark_setup

pyspark_setup.download_files()

sc, spark = pyspark_setup.initialize_spark()

print("########Cross Join###########")

data4 = [
    (1, "raj"),
    (2, "ravi"),
    (3, "sai"),
    (5, "rani")
]

cust = spark.createDataFrame(data4, ["id", "name"]).coalesce(1)
cust.show()

data3 = [
    (1, "mouse"),
    (3, "mobile"),
    (7, "laptop")
]


prod = spark.createDataFrame(data3, ["id", "product"]).coalesce(1)
prod.show()
cross= prod.crossJoin(prod)
cross.show()