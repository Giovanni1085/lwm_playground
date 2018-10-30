# Spark scripts to do a variety of things

This folder contains the Spark stuff for Living with Machines. This is exploratory, pre-refactoring.

## List of contents

TODO

* [readme](README.md) This file.
* [config](config.conf) Application configuration file.
* [requirements](requirements.sh) Spark cluster configuration script to install required Python packages everywhere. Alternatively, the same info in in requirements.txt (should you prefer to ship the pre-compiled dependencies as a zip archive. This solution might be buggy).
* [process_wet_manifest](process_wet_manifest.py) Script to process a CC monthly export and prepare a local data frame.
* [sparkcc](sparkcc.py) Support code for the above.
* [explore_domains](explore_domains.py) Implementation of the algorithm in Spark. Runs offline.
* [bloom_filter](bloom_filter.py) Support code with bloom filters for hostname to domain name lookup.
* [classifier tuning](classifier_tuning.py) Support code to tune the classifier or try different methods. Current code support Logistic Regression and Random Forests.
* [analysis](analysis.py) Support code to run some analysis on the resulting data frames.

## How to:

Data should be already into the HDFS container somewhere.

Clone this repo locally:

    git clone https://github.com/Giovanni1085/lwm_playground.git

Move stuff to HDFS:

    hadoop fs -mkdir /data
    hadoop fs -mkdir /data/wets
    hadoop fs -mkdir /data/tmp
    hadoop fs -copyFromLocal ~/wet.paths /data/wet.paths
    tar -xzvf DSG_NCSC_data_prep/data/domains_curlie.tar.gz
    hadoop fs -copyFromLocal ~/domains_curlie.csv /data/domains_curlie.csv
    hadoop fs -copyFromLocal ~/DSG_NCSC_data_prep/data/* /data/
    hadoop fs -copyFromLocal ~/cc-main-2018-may-jun-jul-domain-vertices.txt.gz /data/vertices.txt.gz
    hadoop fs -copyFromLocal ~/cc-main-2018-may-jun-jul-domain-edges.txt.gz /data/edges.txt.gz

Might need to grant permissions (e.g. Data lake...):

    hdfs dfs -chmod -R 755 /test_data

Edit the config.conf file:

    [tasks]
    seed_file = domains_uk_small_test.csv
    accepted = ["uk."]
    use_accepted = yes
    log_level = WARN
    full_text_threshold = 2000
    use_text_threshold = no
    app_name = NCSC_DSG
    edges_parquet = parquet_edges
    vertices_parquet = parquet_vertices
    num_input_partitions = 1000
    num_output_partitions = 300
    
    [locations]
    which_edges = edges.txt.gz
    which_vertices = vertices.txt.gz
    manifest = wet.paths
    output = wet_processing_test
    final_df_name = final_df_test
    data_folder = /data
    wet_folder = /data/wets
    local_tmp_dir = /data/tmp
    network-folder = /data

Configure your environment by installing requirements.txt and setting some global variables ON ALL NODES:

    export PYSPARK_PYTHON=python
    export PYSPARK_DRIVER_PYTHON=python
    export PYTHONIOENCODING=utf8
        
An easy way to do this is via a script action to be used at setup time or later on. You can use 'requirements.sh' (exposed publicly somewhere, e.g. on GitHub with raw view). If you run the script action, there is no need to use dependencies.zip (which might in any case not work due to boto3).

Submit the process manifest job. This Spark job goes through the manifest, downloads and indexes all files and web pages therein. This script requires a connection to Internet (at least to Amazon s3).

    spark-submit --conf spark.executorEnv.PYTHONHASHSEED=321 run_ocr_eval.py
    nohup spark-submit --conf spark.executorEnv.PYTHONHASHSEED=321 run_ocr_eval.py 1>log.out 2>&1 &
    
Submit the exploration job, no connection to Internet required. Please be aware that Spark memory allocation needs to be properly configured, according to your cluster size, to allow sufficient executor and driver memory (also YARN Java heap memory might need tuning).
    
    spark-submit --py-files bloom_filter.py --conf spark.executorEnv.PYTHONHASHSEED=321 explore_domains.py
    nohup spark-submit --py-files bloom_filter.py --conf spark.executorEnv.PYTHONHASHSEED=321 --conf spark.sql.shuffle.partitions=500 --conf spark.default.parallelism=500 --conf spark.driver.memory=49g --conf spark.driver.cores=7 --conf spark.executor.memory=18g --conf spark.executor.cores=3 --num-executors=40 --conf spark.yarn.executor.memoryOverhead=4096 --conf spark.yarn.driver.memoryOverhead=4096 explore_domains.py 1>log_exp.out 2>&1 &
    
    nohup spark-submit --py-files bloom_filter.py --conf spark.executorEnv.PYTHONHASHSEED=321 --conf spark.sql.shuffle.partitions=500 --conf spark.default.parallelism=500 --conf spark.driver.memory=49g --conf spark.driver.cores=7 --conf spark.executor.memory=18g --conf spark.executor.cores=3 --num-executors=40 --conf spark.yarn.executor.memoryOverhead=4096 --conf spark.yarn.driver.memoryOverhead=4096 explore_domains.py 1>log_exp.out 2>&1 &
    nohup spark-submit --py-files bloom_filter.py --conf spark.executorEnv.PYTHONHASHSEED=321 --conf spark.sql.shuffle.partitions=500 --conf spark.default.parallelism=500 --conf spark.driver.memory=49g --conf spark.driver.cores=7 --conf spark.executor.memory=18g --conf spark.executor.cores=3 --num-executors=60 --conf spark.yarn.executor.memoryOverhead=4096 --conf spark.yarn.driver.memoryOverhead=4096 explore_domains_first_delivery.py 1>log_exp_first.out 2>&1 &
    