import os
import sys
import urllib.request
import ssl
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

# Function to download required files
def download_files():
    data_dir = "data"
    os.makedirs(data_dir, exist_ok=True)

    data_dir1 = "hadoop/bin"
    os.makedirs(data_dir1, exist_ok=True)

    urls_and_paths = {
        "https://raw.githubusercontent.com/saiadityaus1/SparkCore1/master/test.txt": os.path.join(data_dir, "test.txt"),
        "https://github.com/saiadityaus1/SparkCore1/raw/master/winutils.exe": os.path.join(data_dir1, "winutils.exe"),
        "https://github.com/saiadityaus1/SparkCore1/raw/master/hadoop.dll": os.path.join(data_dir1, "hadoop.dll")
    }

    ssl_context = ssl._create_unverified_context()

    for url, path in urls_and_paths.items():
        with urllib.request.urlopen(url, context=ssl_context) as response, open(path, 'wb') as out_file:
            out_file.write(response.read())

# Function to initialize PySpark
def initialize_spark():
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['HADOOP_HOME'] = "hadoop"
    os.environ['JAVA_HOME'] = r'C:\Users\padma\.jdks\corretto-1.8.0_442'

    conf = SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host", "localhost").set("spark.default.parallelism", "1")
    sc = SparkContext(conf=conf)
    spark = SparkSession.builder.getOrCreate()

    return sc, spark

# Run file download only when the module is executed directly (optional)
if __name__ == "__main__":
    download_files()