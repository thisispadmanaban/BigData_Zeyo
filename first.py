import os
import urllib.request
import ssl

data_dir = "data"
os.makedirs(data_dir, exist_ok=True)

data_dir1 = "hadoop/bin"
os.makedirs(data_dir1, exist_ok=True)

urls_and_paths = {
    "https://raw.githubusercontent.com/saiadityaus1/SparkCore1/master/test.txt": os.path.join(data_dir, "test.txt"),
    "https://github.com/saiadityaus1/SparkCore1/raw/master/winutils.exe": os.path.join(data_dir1, "winutils.exe"),
    "https://github.com/saiadityaus1/SparkCore1/raw/master/hadoop.dll": os.path.join(data_dir1, "hadoop.dll")
}

# Create an unverified SSL context
ssl_context = ssl._create_unverified_context()

for url, path in urls_and_paths.items():
    # Use the unverified context with urlopen
    with urllib.request.urlopen(url, context=ssl_context) as response, open(path, 'wb') as out_file:
        data = response.read()
        out_file.write(data)
import os, urllib.request, ssl; ssl_context = ssl._create_unverified_context(); [open(path, 'wb').write(urllib.request.urlopen(url, context=ssl_context).read()) for url, path in { "https://github.com/saiadityaus1/test1/raw/main/df.csv": "df.csv", "https://github.com/saiadityaus1/test1/raw/main/df1.csv": "df1.csv", "https://github.com/saiadityaus1/test1/raw/main/dt.txt": "dt.txt", "https://github.com/saiadityaus1/test1/raw/main/file1.txt": "file1.txt", "https://github.com/saiadityaus1/test1/raw/main/file2.txt": "file2.txt", "https://github.com/saiadityaus1/test1/raw/main/file3.txt": "file3.txt", "https://github.com/saiadityaus1/test1/raw/main/file4.json": "file4.json", "https://github.com/saiadityaus1/test1/raw/main/file5.parquet": "file5.parquet", "https://github.com/saiadityaus1/test1/raw/main/file6": "file6", "https://github.com/saiadityaus1/test1/raw/main/prod.csv": "prod.csv", "https://raw.githubusercontent.com/saiadityaus1/test1/refs/heads/main/state.txt": "state.txt", "https://github.com/saiadityaus1/test1/raw/main/usdata.csv": "usdata.csv", "https://github.com/saiadityaus1/SparkCore1/raw/refs/heads/master/data.orc": "data.orc", "https://github.com/saiadityaus1/test1/raw/main/usdata.csv": "usdata.csv", "https://raw.githubusercontent.com/saiadityaus1/SparkCore1/refs/heads/master/rm.json": "rm.json"}.items()]

# ======================================================================================

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path
os.environ['HADOOP_HOME'] ="hadoop"
os.environ['JAVA_HOME'] = r'C:\Users\padma\.jdks\corretto-1.8.0_442'
######################🔴🔴🔴################################

#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 pyspark-shell'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-avro_2.12:3.5.4 pyspark-shell'
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4 pyspark-shell'


conf = SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.default.parallelism", "1")
sc = SparkContext(conf=conf)

spark = SparkSession.builder.getOrCreate()

spark.read.format("csv").load("data/test.txt").toDF("Success").show(20, False)


#---------------------------- DONT TOUCH ABOVE CODE---------------------
'''
print("===================STARTED====================")
a = 2
print(a)

print("===================AFTER ADDITION===============")

b = a+1
print(b)

c = "Zeyo"
print (c)

d = c+" Analytics"
print (d)

lis = [1,2,3,4]
print(lis)

print("######### RAW RDD LIST ############")
rddin = sc.parallelize(lis)
print(rddin.collect())

print ("#####  ADD RDD with lambda ######")
addrdd = rddin.map(lambda x : x+2)
print (addrdd.collect())

print ("#####  Multiply RDD with lambda ######")
mulrdd = rddin.map(lambda x : x*10)
print (mulrdd.collect())

print ("#####  Subtract RDD with lambda ######")
subrdd = rddin.map(lambda x : x-10)
print (subrdd.collect())

print(" ##### FILTER OPERATION ######")
filrdd = rddin.filter(lambda x: x > 2 )
print (filrdd.collect())

listr = [ "zeyobron" , "zeyo" , "byte" ]
print()
print("=======RAW LIST======")
print (listr)


rddstr = sc.parallelize(listr)
print()
print("======= RDD LIST=======")
print(rddstr.collect())


conrdd = rddstr.map(lambda  x  :  x + "Analytics")
print()
print("======= conrdd LIST=======")
print(conrdd.collect())

filrdd = rddstr.filter(  lambda x : "zeyo" in  x.lower())
print()
print("======= filrdd LIST=======")
print(filrdd.collect())

reprdd =    rddstr.map( lambda x :  x.replace("zeyo","tera"))
print()
print("======= reprdd LIST=======")
print(reprdd.collect())

listr = [   "A~B"    ,    "C~D"    , "E~F" ]
print()
print("===== RAW LIST======")
print(listr)



rddstr = sc.parallelize(listr)
print()
print("===== RDD LIST======")
print(rddstr.collect())



flatdata = rddstr.flatMap( lambda x : x.split("~"))
print()
print("===== flatdata LIST======")
print(flatdata.collect())

print ("############ STATE TASK ############")

listate = [
    "State->TN~City->Chennai" ,
    "State->Kerala~City->Trivandrum"
]

print()
print("===== RAW LIST======")
print(listate)


rddstate = sc.parallelize(listate)
print()
print("===== rddstate LIST======")
print(rddstate.collect())


flatdata = rddstate.flatMap( lambda x : x.split("~"))
print()
print("===== flatdata LIST======")
print(flatdata.collect())



filstate = flatdata.filter( lambda x : 'State' in x)
print()
print("===== filstate LIST======")
print(filstate.collect())


states = filstate.map(lambda x : x.replace( "State->", ""  ))
print()
print("===== states LIST======")
print(states.collect())


filcity =  flatdata.filter( lambda x : 'City' in x)
print()
print("===== filcity LIST======")
print(filcity.collect())


cities = filcity.map(lambda x : x.replace("City->" , ""))
print()
print("===== cities LIST======")
print(cities.collect())


#  full rdd operations

print("=====STARTED======")

a = 2
print(a)


b = a + 2
print(b)


c = "zeyobron"
print(c)


d = c + "Analytics"
print(d)


lisin = [1, 2, 3, 4]
print()
print("===== RAW LIST=====")
print(lisin)


rddin = sc.parallelize(lisin)
print()
print("===== rddin LIST=====")
print(rddin.collect())


addin = rddin.map(lambda x: x + 2)
print()
print("===== addin LIST=====")
print(addin.collect())


filin = rddin.filter(lambda x: x > 2)
print()
print("===== filin LIST=====")
print(filin.collect())


listr = ["zeyobron", "zeyo", "byte"]
print()
print("===== RAW LIST=====")
print(listr)


rddstr = sc.parallelize(listr)
print()
print("===== rddstr LIST=====")
print(rddstr.collect())


conrdd = rddstr.map(lambda x: x + "Analytics")
print()
print("===== conrdd LIST=====")
print(conrdd.collect())


reprdd = rddstr.map(lambda x: x.replace("zeyo", "tera"))
print()
print("===== reprdd LIST=====")
print(reprdd.collect())


listrf = ["A~B", "C~D", "E~F"]
print()
print("===== listrf LIST=====")
print(listrf)


flatrdd = sc.parallelize(listrf)


flatdata = flatrdd.flatMap(lambda x: x.split("~"))
print()
print("===== flatdata LIST=====")
print(flatdata.collect())

# 🔴 STEP 1 -- READ THE FILE

data = sc.textFile("dt.txt")
print()
print("===== RAW DATA====")
data.foreach(print)


# 🔴STEP 2 ----- SPLIT WITH COMMA (MAPSPLIT)

mapsplit = data.map(lambda x : x.split(","))


from collections import namedtuple

# 🔴STEP 3 ---- DEFINE COLUMNS


columns = namedtuple('columns',['tno','tdate','amount','category','product','mode'])


# 🔴STEP 4 --- IMPOSE EACH DATA SPLIT TO THE COLUMNS


schemardd = mapsplit.map(lambda x : columns(x[0],x[1],x[2],x[3],x[4],x[5]))



# 🔴STEP 5 ---- FILTER  REQUIRED PRODUCT COLUMN

filterrdd = schemardd.filter(lambda x : 'Gymnastics' in x.product)
print()
print("===== filter rdd DATA====")
filterrdd.foreach(print)

listr = [ "hadoop~spark~hive~sqoop" ]
rddstr = sc.parallelize(listr)
print(rddstr.collect())

flatlistr = rddstr.flatMap(lambda x: x.split("~"))
print(flatlistr.collect())

fil = flatlistr.map(lambda x:x.upper())
condata = fil.map(lambda x: "Tech->" + x + " Trainer->Sai" )
condata.foreach(print)


#🔴 SQL PRE REQUISITE CODE

data = [
    (0, "06-26-2011", 300.4, "Exercise", "GymnasticsPro", "cash"),
    (1, "05-26-2011", 200.0, "Exercise Band", "Weightlifting", "credit"),
    (2, "06-01-2011", 300.4, "Exercise", "Gymnastics Pro", "cash"),
    (3, "06-05-2011", 100.0, "Gymnastics", "Rings", "credit"),
    (4, "12-17-2011", 300.0, "Team Sports", "Field", "cash"),
    (5, "02-14-2011", 200.0, "Gymnastics", None, "cash"),
    (6, "06-05-2011", 100.0, "Exercise", "Rings", "credit"),
    (7, "12-17-2011", 300.0, "Team Sports", "Field", "cash"),
    (8, "02-14-2011", 200.0, "Gymnastics", None, "cash")
]

df = spark.createDataFrame(data, ["id", "tdate", "amount", "category", "product", "spendby"])
df.show()





data2 = [
    (4, "12-17-2011", 300.0, "Team Sports", "Field", "cash"),
    (5, "02-14-2011", 200.0, "Gymnastics", None, "cash"),
    (6, "02-14-2011", 200.0, "Winter", None, "cash"),
    (7, "02-14-2011", 200.0, "Winter", None, "cash")
]

df1 = spark.createDataFrame(data2, ["id", "tdate", "amount", "category", "product", "spendby"])
df1.show()




data4 = [
    (1, "raj"),
    (2, "ravi"),
    (3, "sai"),
    (5, "rani")
]



cust = spark.createDataFrame(data4, ["id", "name"])
cust.show()

data3 = [
    (1, "mouse"),
    (3, "mobile"),
    (7, "laptop")
]

prod = spark.createDataFrame(data3, ["id", "product"])
prod.show()



#🔴  *SPARK DATA FRAME READS*





csvdf = spark.read.format("csv").option("header","true").load("usdata.csv")

print()

print("======== CSV DF==============")

print()

csvdf.show()








parquetdf =  spark.read.format("parquet").load("file5.parquet")

print()

print("======== parquetdf ==============")

print()

parquetdf.show()







orcdf =   spark.read.format("orc").load("data.orc")

print()

print("======== orcdf ==============")

print()

orcdf.show()









jsondf =  spark.read.format("json").load("file4.json")

print()

print("======== jsondf ==============")

print()

jsondf.show()



# PROCESS


csvdf = spark.read.format("csv").option("header","true").load("usdata.csv")
print()
print("======== CSV DF==============")
print()
csvdf.show()


csvdf.createOrReplaceTempView("zeyo")

procdf = spark.sql("select * from zeyo where state='LA'")
procdf.show()

'''

data = [
    (0, "06-26-2011", 300.4, "Exercise", "GymnasticsPro", "cash"),
    (1, "05-26-2011", 200.0, "Exercise Band", "Weightlifting", "credit"),
    (2, "06-01-2011", 300.4, "Exercise", "Gymnastics Pro", "cash"),
    (3, "06-05-2011", 100.0, "Gymnastics", "Rings", "credit"),
    (4, "12-17-2011", 300.0, "Team Sports", "Field", "cash"),
    (5, "02-14-2011", 200.0, "Gymnastics", None, "cash"),
    (6, "06-05-2011", 100.0, "Exercise", "Rings", "credit"),
    (7, "12-17-2011", 300.0, "Team Sports", "Field", "cash"),
    (8, "02-14-2011", 200.0, "Gymnastics", None, "cash")
]

df = spark.createDataFrame(data, ["id", "tdate", "amount", "category", "product", "spendby"])
df.show()

data2 = [
    (4, "12-17-2011", 300.0, "Team Sports", "Field", "cash"),
    (5, "02-14-2011", 200.0, "Gymnastics", None, "cash"),
    (6, "02-14-2011", 200.0, "Winter", None, "cash"),
    (7, "02-14-2011", 200.0, "Winter", None, "cash")
]

df1 = spark.createDataFrame(data2, ["id", "tdate", "amount", "category", "product", "spendby"])
df1.show()


data4 = [
    (1, "raj"),
    (2, "ravi"),
    (3, "sai"),
    (5, "rani")
]


cust = spark.createDataFrame(data4, ["id", "name"])
#cust.show()

data3 = [
    (1, "mouse"),
    (3, "mobile"),
    (7, "laptop")
]

prod = spark.createDataFrame(data3, ["id", "product"])
#prod.show()

df.createOrReplaceTempView("df")
# df1.createOrReplaceTempView("df1")
# cust.createOrReplaceTempView("cust")
# prod.createOrReplaceTempView("prod")


# spark.sql("select id,tdate from df").show()

# spark.sql("select * from df where category= \"Exercise\"").show()
# spark.sql("select id,tdate,category,spendby from df where category = 'Exercise' and spendby = 'cash'").show()
data = [
    ("00000", "06-26-2011", 200, "Exercise", "GymnasticsPro", "cash"),
    ("00001", "05-26-2011", 300, "Exercise", "Weightlifting", "credit"),
    ("00002", "06-01-2011", 100, "Exercise", "GymnasticsPro", "cash"),
    ("00003", "06-05-2011", 100, "Gymnastics", "Rings", "credit"),
    ("00004", "12-17-2011", 300, "Team Sports", "Field", "paytm"),
    ("00005", "02-14-2011", 200, "Gymnastics", None, "cash")
]

df5 = spark.createDataFrame(data, ["id", "tdate", "amount", "category", "product", "spendby"])
df5.show()

procdf = df5.selectExpr("id",
                       "tdate",
                       "amount+100 as amount",
                       "upper(category) as category",
                       "concat(product,'~zeyo') as product",
                       "spendby",
                        "case when spendby='cash' then 0 else 1 end as status"
                       )

procdf.show();


data = [
    ("00000", "06-26-2011", 200, "Exercise", "GymnasticsPro", "cash"),
    ("00001", "05-26-2011", 300, "Exercise", "Weightlifting", "credit"),
    ("00002", "06-01-2011", 100, "Exercise", "GymnasticsPro", "cash"),
    ("00003", "06-05-2011", 100, "Gymnastics", "Rings", "credit"),
    ("00004", "12-17-2011", 300, "Team Sports", "Field", "paytm"),
    ("00005", "02-14-2011", 200, "Gymnastics", None, "cash")
]


print ("---------------Task1 Completion---------------")
df6 = spark.createDataFrame(data, ["id", "tdate", "amount", "category", "product", "spendby"])
df6.show()


procdf = df6.selectExpr(
    "cast(id as int)  as id",
    "split(tdate,'-')[2] as year",
    "amount+100 as amount",
    "upper(category) as category",
    "concat(product,'~zeyo') as product",
    "spendby",
    "case when spendby='cash' then 0 else 1 end as status"
)

procdf.show()