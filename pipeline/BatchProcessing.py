# -*- coding: utf-8 -*-

#from db_connection import MongoDB
#import preprocess 
#import bpp_preprocess_function
#import parallelize_all_convos
import luigi
import json
import pandas as pd
import math
import json
import logging
import sys
import os
import os.path
import threading
import pymongo
import requests
import logging
import math
import luigi
import time
from pymongo import MongoClient
from flashtext.keyword import KeywordProcessor
import json
#Custom class
from DBConnection import MongoDB
import MultiCoreProcess
from MultiCoreProcess import parallelize_all_convos
from PreProcessing import bpp_preprocess

class BatchProcessing(luigi.Task):
    global data,logger,mongoObject
    
    #Fetch parameters from terminal
    batch_count=luigi.IntParameter(default=10)
    label=luigi.Parameter(default='BJP')
    limit=luigi.IntParameter(default='1000')
    
    with open('../data.json','r') as fin:
        data=json.load(fin)
        fin.close()
    
    #Read the config file for logging
    #Get the root handler logger
    logger = logging.getLogger('logging.conf')
    def __init__(self,*args,**kwargs):
        luigi.Task.__init__(self,*args,**kwargs)
        self.connect_mongo()
        #print("Starting BatchProcessing Task with process type {}".format(process_type))
        #####################################################################################
    def connect_mongo(self):
        #Show information and log whether pipeline is being run for SSF or ITD system and set config file accordingly
        self.log_and_print("Setting config variables and Executing pipeline for SSF data")
         #Create the pipeline to import data from MongoDB
        self.pipeline=[
                     {"$match":{"$and":[#{data['SSF_COLLECTION_CONFIG']['text_column_array_name']:{"$exists":True}},
                                        {data['COLLECTION_CONFIG']['category_field_name']:{"$exists":False}}]}},
                     {"$limit":self.limit},
                     {"$project": {"id":"$_id","location" : "$Location", "likes" : "$Likes","retweet":"$Retweet","tweet":"$Tweet","created_at":"$Created At","name":"$Name"}}
                ]
        #Default : Process dataset for BJP tweets
        self.collection_name=data['COLLECTION_CONFIG']['collection_BJP']
        logger.error('Processing {} tweets with batch count {} and limit {}'.format(self.label,self.batch_count,self.limit))
        print('Processing {} tweets with batch count {} and limit {}'.format(self.label,self.batch_count,self.limit))
        if self.label=='Congress':
            self.collection_name=data['COLLECTION_CONFIG']['collection_Congress']    
        elif self.label=='BJP':
            self.collection_name=data['COLLECTION_CONFIG']['collection_BJP']
        self.mongoObject=MongoDB(data['COLLECTION_CONFIG']['dbname'],self.collection_name)
        self.cursor=self.mongoObject.getCollection().aggregate(self.pipeline,allowDiskUse=True)
        logger.info('Mongo Object initiated loading {}'.format(self.mongoObject.getCollection()))    
    
    #Util function to print and log same information to avoid redundancy
    def log_and_print(self,x):
        #print(x)
        logger.info(x)
        
    def run(self):
        try:
            df_tweet=pd.DataFrame(list(self.cursor))
            self.log_and_print('Shape of dataframe {}'.format(df_tweet.shape[0]))
            logger.info('Total {} records fetched from {}'.format(df_tweet.shape[0],self.mongoObject.getCollection()))
            #logger.info(df_tweet.head())
            self.log_and_print('Shape of dataframe df_tweet : {}'.format(df_tweet.shape[0]))
            df_tweet=df_tweet.loc[~df_tweet["tweet"].isnull()]
            
        except BaseException as br:
            logger.error('Some error occured while fetching the records {}'.format(br))
            sys.exit(0) 
        
        #Set index of dataset to primary key
        #df_tweet.set_index("_id",inplace=True)
        
        #Split the convos column in sub column
        allConvos_df=df_tweet['tweet']
        try:
            #Parallelize the preprocessing
            print("Starting Multicore pre-processing with {} records and {} batches".format(allConvos_df.shape[0],self.batch_count))
            parallelize_processing=parallelize_all_convos()   
            bpp_preprocess_df = parallelize_processing.parallelize_dataframe(allConvos_df,self.batch_count)
            #bpp_preprocess_df.set_index('_id')
            logger.info("Shape of bpp_preprocess_df data is {}".format(bpp_preprocess_df.head()))
            print(bpp_preprocess_df.head())
            print("Preprocessing complete for {} batches. \nWriting output results to MongoDB".format(self.batch_count))
            id_list=df_tweet['id']
            for index,row in zip(id_list,bpp_preprocess_df):
                try:
                    result=self.mongoObject.getCollection().update_one({'_id':index},{'$set':{data['COLLECTION_CONFIG']['category_field_name']:row}},upsert=True)
                except BaseException as be:
                     logger.error('Error while updating record {} convo : {} {}'.format(index,convo,row))
            print("Preprocessing complete for all records in {} batches \nDone :)".format(self.batch_count))
        except BaseException as br:
            logger.error("Failed while processing dataset {}".format(br))
            sys.exit(0)
        except BaseException as br:
            logger.error("Failed while processing dataset {}".format(br))
            sys.exit(0)
            
    def output(self):
        return luigi.LocalTarget('batchPreprocessing.txt')      
        
if __name__=="__main__":
    luigi.run()