"""
Assess OCR quality (outputs a data structure to allow to play with results separately, plus a summary report)
"""
from __future__ import division

__authors__ = "Giovanni Colavizza"

import configparser, os, logging, gc, codecs
from bs4 import BeautifulSoup

from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import SQLContext

config = configparser.ConfigParser()
config.read("config.conf")

REPARTITION_VALUE = 100 # it is quite crucial to properly partition dataframes (especially after the machine learning part, as it tends to skew them). This value should be roughly 2-4x the number of CPUs (or executors) available.
APP_NAME = config.get("tasks", "app_name")
num_input_partitions = int(config.get("tasks", "num_input_partitions"))
num_output_partitions = int(config.get("tasks", "num_output_partitions"))

LOGGING_FORMAT = '%(asctime)s %(levelname)s %(name)s: %(message)s'
log_level = config.get("tasks", "log_level") # Set new log level: "ALL", "DEBUG", "ERROR", "FATAL", "INFO", "OFF", "TRACE", "WARN"

s = SparkSession.builder.appName(APP_NAME).getOrCreate()
s.sparkContext.setLogLevel(log_level)
logging.basicConfig(level=log_level, format=LOGGING_FORMAT)
logger = s.sparkContext._jvm.org.apache.log4j.LogManager.getLogger(APP_NAME)

# 1) get all files of interest
# TODO: find a better spark way to do this

"""
source_files = list()
for root, folders, files in os.walk(config.get("locations", "data_folder")):
    for fname in files:
        if ".xml" in fname and not "mets.xml" in fname: # only consider data files
            source_files.append(os.path.join(root,fname))
"""
source_files = s.sparkContext.wholeTextFiles(os.path.join(config.get("locations", "data_folder"),"*/*/*/*.xml")).map(lambda x: x[0]).collect()
source_files = [fname for fname in source_files if not "_mets" in fname]
print("Number of files:",str(len(source_files)))
source_files = s.sparkContext.parallelize(source_files)

# 2) define the function which parses the file and exports a dictionary of information
def parse_ocr_meta(files):
    "from a valid xml filename, takes out OCR metadata and exposes it as a df row"

    for filename in files:
        print(filename)
        # open file with bs4
        t = s.sparkContext.textFile(filename)
        soup = BeautifulSoup("\n".join(t.collect()))
        ocr_meta = soup.find("ocrprocessingstep")
        ocr_text = ocr_meta.processingstepsettings.text
        result_list = list()
        for line in ocr_text.splitlines():
            line = line.strip()
            line = line.replace("%","")
            result_list.append(line.split(": ")[1])
        # structure:
        # 0: Character Count: 48237
        # 1: Predicted Word Accuracy: 68.9%
        # 2: Suspicious Character Count: 9074
        # 3: Word Count: 8840
        # 4: Suspicious Word Count: 4350
        # 5: width: 4583
        # 6: height: 6189
        # 7: xdpi: 300
        # 8: ydpi: 300
        # 9: source-image: //bl-dun-stor4.bsolbl.local/data01/blend4/2016-01-18_07_04/2016-01-18_07_04_00265.tif
        yield filename,int(result_list[0]),float(result_list[1]),int(result_list[2]),int(result_list[3]),int(result_list[4])
        int(result_list[5]),int(result_list[6]),int(result_list[7]),int(result_list[8])

# 3) apply to the dataset

output_schema = StructType([
    StructField("file_name", StringType(), True),
    StructField("character_count", LongType(), True),
    StructField("predicted_word_accuracy", FloatType(), True),
    StructField("suspicious_character_count", LongType(), True),
    StructField("word_count", LongType(), True),  # from the domain_list.
    StructField("suspicious_word_count", LongType(), True),
    StructField("width", LongType(), True),
    StructField("height", LongType(), True),
    StructField("xdpi", LongType(), True),
    StructField("ydpi", LongType(), True)
    ])

output = source_files.mapPartitions(parse_ocr_meta) \
            .distinct()

print("Number of output records:",str(output.count()))

sqlc = SQLContext(sparkContext=s.sparkContext)

sqlc.createDataFrame(output, schema=output_schema) \
            .coalesce(REPARTITION_VALUE) \
            .write \
            .mode('overwrite') \
            .parquet(os.path.join(config.get("locations", "export_folder"), config.get("locations", "output")))

print("END")