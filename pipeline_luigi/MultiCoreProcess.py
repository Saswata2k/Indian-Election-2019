# -*- coding: utf-8 -*-

import pandas as pd
import multiprocessing
import nltk
import numpy as np
from PreProcessing import bpp_preprocess
import logging
from itertools import product

class parallelize_all_convos:
    num_cores=2
    logger=logging.getLogger(__name__)
    
    def parallelize_dataframe(self,df,batch_count):
        self.logger.info("Inside parallelize all convos. Starting processing pool map with {} batches".format(batch_count))
        dataframe = np.array_split(df,batch_count)
        pool = multiprocessing.Pool(self.num_cores)
        df = pd.concat(pool.map(self.mapping, dataframe))
        #self.logger.info(columnNames)    
        self.logger.info("Executed Processing pool map successfully")
        pool.close()
        pool.join()
        return df
    
    @staticmethod
    def mapping(dataframe):
        #english_stopwords=stopwords.words("english")
        #stopwords_list=list(english_stopwords) 
        #pass the stopwords list in argument should the need arise to remove the stopwords
        #for now we will keep the stopwords
        basicprocess=bpp_preprocess()
        try:
            #for col in dataframe.columns:
            dataframe = dataframe.apply(lambda x: basicprocess.preprocess_text(x))
        except BaseException as br:
                logger.error('Error in parallelizing {}'.format(br))
        return dataframe