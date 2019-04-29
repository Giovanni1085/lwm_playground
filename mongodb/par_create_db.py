#!/usr/bin/python
# -*- coding: UTF-8 -*-

# Parallel version of create_db.py written by Giovanni Colavizza.
# run with 4 cores:
# mpirun -np 4 python par_create_db.py

# Parses the book collection (including metadata) and creates a local MongoDB
__author__ = """Giovanni Colavizza, Kasra Hosseini"""

import json, codecs, os, sys
from configparser import ConfigParser
from mpi4py import MPI
from pymongo import MongoClient
from pymongo import HASHED, ASCENDING

# ----------- connect_mongoClient
def connect_mongoClient(collection_name="lwm_books", auth_req=True):
    """
    create a MongoClient object, delete existing "metadata" and "volumes"
    :param collection_name:
    :param auth_req:
    :return:
    """
    # Mongo connection
    config = ConfigParser(allow_no_value=False)
    config.read("mongo_config.conf")
    mongo_db = config.get(collection_name, 'db-name')
    mongo_user = config.get(collection_name, 'username')
    mongo_pwd = config.get(collection_name, 'password')
    mongo_auth = config.get(collection_name, 'auth-db')
    mongo_host = config.get(collection_name, 'db-host')
    mongo_port = config.get(collection_name, 'db-port')
    client = MongoClient(mongo_host)
    db = client[mongo_db]
    if auth_req:
        db.authenticate(mongo_user, mongo_pwd, source=mongo_auth)

    # select where to act in Mongo
    db.drop_collection("metadata")
    db.drop_collection("volumes")
    collection_metadata = db.metadata
    collection_volumes = db.volumes

    return collection_metadata, collection_volumes

# ----------- split4mpi
def split4mpi(list_of_jobs, num_procs):
    """
    split list_of_jobs into num_procs chunks
    :param list_of_jobs:
    :param num_procs:
    :return:
    """
    return [list_of_jobs[i::num_procs] for i in range(num_procs)]

# =======================================

# ----------input
# how many books to process before batch ingesting into Mongo
BATCH_SIZE = 1000
MODE_TEST = True
# ----------end input

# default communicator
COMM = MPI.COMM_WORLD

if COMM.rank == 0:
    # Data locations
    if MODE_TEST:
        metadata_file = os.path.join("test_data", "json_book_examples_metadata.json")
        data_folder = os.path.join("test_data", "examples_books_fulltext")
        auth_req = False
    else:
        base_storage_path = os.path.join("mnt", "extra_storage", "downloads")
        metadata_file = os.path.join(base_storage_path, "book_data.json")
        data_folder = os.path.join(base_storage_path, "json")
        auth_req = True

    # read the metadata file
    try:
        metadata = json.loads(codecs.open(metadata_file).read())
    except Exception as err:
        sys.exit("Metadata file could not be loaded. Error: %s" % err)

    # split metadata into number of available cores
    metadata = split4mpi(metadata, COMM.size)

else:
    if MODE_TEST:
        auth_req = False
    else:
        auth_req = True
    data_folder = None
    metadata = None

# Mongo connection
collection_metadata, collection_volumes = connect_mongoClient(collection_name="lwm_books", auth_req=auth_req)

# broadcast data_folder info
COMM.barrier()
data_folder = COMM.bcast(data_folder, root=0)
metadata = COMM.scatter(metadata, root=0)

# Start getting text from files
processed_volume_data = list()
books_count = 0
volumes_count = 0

for book in metadata:
    identifier = book["identifier"]
    foldername = identifier[:4]
    current_folder = os.path.join(data_folder, foldername)
    files = [f for f in os.listdir(current_folder) if f.startswith(identifier)]
    print(files)
    volumes = list()
    for f in files:
        number = 0 # default for volumes without number
        try:
            _, number, _ = f.split("_")
            number = int(number)  # cast volume number
        except:
            print("Missing volume number")
        with codecs.open(os.path.join(current_folder,f)) as read_in:
            text_lines = json.loads(read_in.read())
        #full_text = " ".join(l[1].strip() for l in text_lines)
        #full_text = " ".join(full_text.split())
        processed_volume_data.append({"number": number, "text_lines":text_lines, "identifier": identifier}) # "text_full":full_text,
        volumes_count += 1
    book["number_of_volumes"] = len(files)
    books_count += 1
    if len(processed_volume_data) == BATCH_SIZE:
        collection_volumes.insert_many(processed_volume_data)
        del processed_volume_data[:]
        del processed_volume_data
        processed_volume_data = list()
        print("Inserted and deleted list")

if len(processed_volume_data) > 0:
    collection_volumes.insert_many(processed_volume_data)
collection_metadata.insert_many(metadata)
print("Books and volumes:", books_count, volumes_count)

# Create indexes
collection_volumes.create_index([('identifier', HASHED)], background=True)
collection_metadata.create_index([('identifier', HASHED)], background=True)

"""
# to test the created database:
import pymongo

myclient = pymongo.MongoClient('mongodb://localhost:27017/')
mydb = myclient["lwm_books"]
mycol = mydb['volumes']
for x in mycol.find().sort('identifier'): print(x['identifier'])
"""
