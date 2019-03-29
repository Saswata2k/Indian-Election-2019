# -*- coding: utf-8 -*-
import re
from flashtext.keyword import KeywordProcessor
import nltk
import time
from string import digits
import json
from DBConnection import MongoDB
import pandas as pd
import logging 
import os.path
import string
import numpy as np
from googletrans import Translator  
from html2text import HTML2Text

class bpp_preprocess:
    
    global data
    logger=logging.getLogger(__name__)
    
    with open('data.json','r') as fin:
        data=json.load(fin)
        fin.close()
        
    
    def translate_tweet(self,x):
        # Language Translator
        translator = Translator() # Create object of Translator.
        try:
            translated = translator.translate(x) 
        except:
            translated=x
            return x
        return translated.text
    
    #Tokenize Terms
    def tokenize_term(self,x):
        sentence=self.keyword_processor_token.replace_keywords(x)
        return sentence
            
    def regex_filtering(self,text):
        if text:
            
            #removing all retweers
            text=re.sub(r"RT @[\w]*:",'',text)
            
            #removing url links
            text=re.sub(r"http\S+", "", text)
            
            #removing all non word character. Apart from letters and digits, we will keep # also
            text=re.sub(r"([^a-zA-Z0-9#])+",' ',text)
            
            #removing extra whitespace
            text=re.sub(r"\s\s+",' ',text)
            
            text=text.strip()
            return text
    
    def preprocess_text(self,tweet):
        #If input text is null, return null as it is
        if tweet:
            clean_tweet=self.translate_tweet(tweet)
            #text=self.html_to_text(clean_tweet)
            filtered_tweet=self.regex_filtering(clean_tweet)
            #filtered_tweet=self.clean_tweets(clean_tweet)
            return filtered_tweet
        else:
            return tweet