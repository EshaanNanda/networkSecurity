import os
import sys
import json

from dotenv import load_dotenv
load_dotenv()

MONGODB_URL=os.getenv('MONGODB_URL')

import certifi 
ca=certifi.where()

import pandas as pd 
import numpy as np
import pymongo

from networkSec.logging import logger
from networkSec.exception.exception import NetworkSecurityException


class NetworkDataExtract():
    def __init__(self):
        try:
            pass
        except Exception as e:
            raise NetworkSecurityException(e,sys)
        
    def to_json(self,file_path):
        try:
            data=pd.read_csv(file_path)
            data.reset_index(inplace=True,drop=True)
            #mongodb needs json it wont take csv like this 
            records=list(json.loads(data.T.to_json()).values())
            return records
        except Exception as e:
            raise NetworkSecurityException(e,sys)
        
    def insert_data(self,records,database,collection):
        try:
            self.database=database
            self.records=records
            self.collection=collection

            self.mongo_client=pymongo.MongoClient(MONGODB_URL)
            self.database=self.mongo_client[self.database]
            self.collection=self.database[self.collection]
            self.collection.insert_many(self.records)
            
            return(len(self.records))
        except Exception as e:
            raise NetworkSecurityException(e,sys)
        
if __name__=="__main__":
    FILE_PATH="NetworkData\phisingData.csv"
    DATABASE='EshaanNanda'
    Collection="NetworkData"
    networkobj=NetworkDataExtract()
    records=networkobj.to_json(file_path=FILE_PATH)
    no_of_records=networkobj.insert_data(records,DATABASE,Collection)
    print(no_of_records)
