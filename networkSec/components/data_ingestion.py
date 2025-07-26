#read data frm mongodb
#create in feature store
#split data in test train
#save in ingested
from networkSec.entity.artifact_entity import DataIngestionArtifact
from networkSec.exception.exception import NetworkSecurityException
from networkSec.logging.logger import logging
from networkSec.entity.config_entity import DataIngestionConfig
import os 
import sys 
import pymongo
import numpy as np
from typing import List
import pandas as pd
from sklearn.model_selection import train_test_split
from dotenv import load_dotenv
load_dotenv()

MONGODB_URL=os.getenv("MONGODB_URL")



class DataIngestionComponent:
    def __init__(self,data_ingestion_config:DataIngestionConfig):
        try:
            self.data_ingestion_config=data_ingestion_config

            
        except Exception as e:
            raise NetworkSecurityException(e,sys)

    def export_dataframe(self):
        # read data from mongodb
        try:
            database_name=self.data_ingestion_config.database_name
            collection_name=self.data_ingestion_config.collection_name
            self.mongo_client=pymongo.MongoClient(MONGODB_URL)
            collection=self.mongo_client[database_name][collection_name]

            df=pd.DataFrame(list(collection.find()))
            if "_id" in df.columns.to_list():
                df.drop(columns=["_id"],axis=1)
            
            df.replace({"na":np.nan},inplace=True)

            return df

        except Exception as e:
            raise NetworkSecurityException(e,sys)

    def export_feature_store(self,dataframe:pd.DataFrame):
        try:
            feature_store_file_path=self.data_ingestion_config.feature_store_file_path

            dir_path=os.path.dirname(feature_store_file_path)
            os.makedirs(dir_path,exist_ok=True)
            dataframe.to_csv(feature_store_file_path,index=False,header=True)

            return dataframe
        except Exception as e:
            raise NetworkSecurityException(e,sys)


    def train_test(self,datafeame:pd.DataFrame):
        try:
            train_set,test_set=train_test_split(datafeame,test_size=self.data_ingestion_config.train_test_split_ratio)
            logging.info("Performed train test split")
            logging.info("Exited the train_test method")

            dir_path=os.path.dirname(self.data_ingestion_config.training_file_path)
            os.makedirs(dir_path,exist_ok=True)

            logging.info(f"Exporting train anf test file path")

            train_set.to_csv(self.data_ingestion_config.training_file_path,index=False,header=True)
            test_set.to_csv(self.data_ingestion_config.testing_file_path,index=False,header=True)
            logging.info("Made dir for train & test set")

        except Exception as e:
            raise NetworkSecurityException(e,sys)

    def initiate_data_ingestion(self):
        try:
            dataframe=self.export_dataframe()
            dataframe=self.export_feature_store(dataframe)
            self.train_test(dataframe)

            dataingestionartifact=DataIngestionArtifact(trained_file_path=self.data_ingestion_config.training_file_path,test_file_path=self.data_ingestion_config.testing_file_path)
            
            return dataingestionartifact
        
        except Exception as e:
            raise NetworkSecurityException(e,sys)