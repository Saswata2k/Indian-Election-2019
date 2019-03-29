import pymongo
import requests
import logging
import math
import luigi
import time
import json
from pymongo import MongoClient

class MongoDB:    
    global data
    logger=logging.getLogger(__name__)  
    with open('data.json','r') as fin:
        data=json.load(fin)
        fin.close()
    logger=logging.getLogger(__name__)
    mongo_url = data['DATABASE_CONFIG']['host_port']
    #user=data['DATABASE_CONFIG']['user']
    #password=data['DATABASE_CONFIG']['password']
    #auth_mechanism=data['DATABASE_CONFIG']['AUTH_MECHANISM']
    #auth_source=data['DATABASE_CONFIG']['authSource']
    
    def __init__(self, db, collection):
        
        try:          
            self.client = MongoClient(self.mongo_url,unicode_decode_error_handler='ignore',connectTimeoutMS=432000,maxIdleTimeMS=432000,socketTimeoutMS=432000,
                                     serverSelectionTimeoutMS=43200)
            #,connectTimeoutMS=6000, socketTimeoutMS=6000,socketKeepAlive=True,serverSelectionTimeoutMS=6000
            self.db = self.client[db]
            self.collection = self.db[collection]
        except BaseException as be:
            logger.exception("Some error occured in Database Connection {}".format(be))

    def getDb(self):
        return self.db

    def getCollection(self):
        return self.collection