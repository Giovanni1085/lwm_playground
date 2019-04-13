#!/usr/bin/python
# -*- coding: UTF-8 -*-

# Parses the book collection (including metadata) and creates a local MongoDB
__author__ = """Giovanni Colavizza"""

import json, codecs, os
from configparser import ConfigParser
from pymongo import MongoClient
from pymongo import HASHED, ASCENDING

MODE_TEST = True
BATCH_SIZE = 1000 # how many books to process before batch ingesting into Mongo

# Data locations
# TODO: parametrize
metadata_file = "book_data.json"
data_folder = "json"
if MODE_TEST:
    # Note there are 15 books and 18 volumes in the test dataset
    metadata_file = "test_data/json_book_examples_metadata.json"
    data_folder = "test_data/examples_books_fulltext"

# read the metadata file
try:
    metadata = json.loads(codecs.open(metadata_file).read())
except:
    print("Metadata file could not be loaded")

# Mongo connection
db = "lwm_books" # this is in localhost
config = ConfigParser(allow_no_value=False)
config.read("mongo_config.conf")
mongo_db = config.get(db, 'db-name')
mongo_user = config.get(db, 'username')
mongo_pwd = config.get(db, 'password')
mongo_auth = config.get(db, 'auth-db')
mongo_host = config.get(db, 'db-host')
mongo_port = config.get(db, 'db-port')
client = MongoClient(mongo_host)
db = client[mongo_db]
db.authenticate(mongo_user, mongo_pwd, source=mongo_auth)

# select where to act in Mongo
db.drop_collection("books")
collection = db.books

# Start getting text from files
# TODO: parallelize
processed_data = list()

for book in metadata:
    identifier = book["identifier"]
    foldername = identifier[:4]
    current_folder = os.path.join(data_folder,foldername)
    files = [f for f in os.listdir(current_folder) if f.startswith(identifier)]
    print(files)
    volumes = list()
    for f in files:
        _, number, _ = f.split("_")
        number = int(number) # cast volume number
        text_lines = json.loads(codecs.open(os.path.join(current_folder,f)).read())
        full_text = " ".join(l[1].strip() for l in text_lines)
        full_text = " ".join(full_text.split())
        volumes.append({number:{"text_lines":text_lines,"text_full":full_text}})
    book["volumes"] = volumes
    processed_data.append(book)
    if len(processed_data) == BATCH_SIZE:
        collection.insert_many(processed_data)
        processed_data = list()

# Create indexes
collection.create_index([('identifier', HASHED)], background=True)