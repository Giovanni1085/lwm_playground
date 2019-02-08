# Spark scripts to do a variety of things

This folder contains the Spark stuff for Living with Machines. This is exploratory, pre-refactoring.

## List of contents

TODO

* [readme](README.md) This file.
* [config](config.conf) Application configuration file.
* [requirements](requirements.sh) Spark cluster configuration script to install required Python packages everywhere. Alternatively, the same info in in requirements.txt (should you prefer to ship the pre-compiled dependencies as a zip archive. This solution might be buggy).

## How to:

Data should be already into the HDFS container somewhere.

Clone this repo locally:

    git clone https://github.com/Giovanni1085/lwm_playground.git

Edit the config.conf file:

    [tasks]
    log_level = WARN
    app_name = LWM_EXPLORE_OCR
    num_input_partitions = 1000
    num_output_partitions = 1000
    
    [locations]
    output = ocr_df
    export_folder = /BNA/exports
    data_folder = /BNA_test

Configure your environment by installing requirements.txt and setting some global variables ON ALL NODES:

    export PYSPARK_PYTHON=python
    export PYSPARK_DRIVER_PYTHON=python
    export PYTHONIOENCODING=utf8
        
An easy way to do this is via a script action to be used at setup time or later on. You can use 'requirements.sh' (exposed publicly somewhere, e.g. on GitHub with raw view). If you run the script action, there is no need to use dependencies.zip (which might in any case not work due to boto3).

Submit the process manifest job. This Spark job goes through the manifest, downloads and indexes all files and web pages therein. This script requires a connection to Internet (at least to Amazon s3).

    spark-submit --conf spark.executorEnv.PYTHONHASHSEED=321 run_ocr_eval.py
    nohup spark-submit --conf spark.executorEnv.PYTHONHASHSEED=321 run_ocr_eval.py 1>log.out 2>&1 &
    
    nohup spark-submit --conf spark.executorEnv.PYTHONHASHSEED=321 --conf spark.sql.shuffle.partitions=2000 --conf spark.default.parallelism=2000 --conf spark.driver.memory=24g --conf spark.driver.cores=7 --conf spark.executor.memory=21g --conf spark.executor.cores=3 --num-executors=12 --conf spark.yarn.executor.memoryOverhead=4096 --conf spark.yarn.driver.memoryOverhead=4096 run_ocr_eval.py 1>log.out 2>&1 &
    
    nohup spark-submit --conf spark.executorEnv.PYTHONHASHSEED=321 --conf spark.sql.shuffle.partitions=10000 --conf spark.default.parallelism=10000 --conf spark.driver.memory=49g --conf spark.driver.cores=7 --conf spark.executor.memory=21g --conf spark.executor.cores=3 --num-executors=240 --conf spark.yarn.executor.memoryOverhead=4096 --conf spark.yarn.driver.memoryOverhead=4096 --conf spark.rpc.message.maxSize=256m run_ocr_eval.py 1>log.out 2>&1 &
    
Example of output:

    ('Number of output records:', '12172')
    Report
    ('Total files:', '15934')
    ('Total ALTO files:', '13112')
    ('Total METS files:', '2659')
    ('Total UK_newspaper files:', '0')    