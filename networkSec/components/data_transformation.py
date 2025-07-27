import sys,os
import numpy as np
import pandas as pd 
from sklearn.impute import KNNImputer
from sklearn.pipeline import Pipeline
from networkSec.constants.training_pipeline import DATA_TRANSFORMATION_IMPUTER_PARAMS, TARGET_COLUMN
#from networkSec.constants.training_pipeline import DATA_TRANSFORMATION_IMPUTER_PARAMS
from networkSec.entity.config_entity import DataTransformationConfig
from networkSec.entity.artifact_entity import DataValidationArtifact,DataTransformationArtifact
from networkSec.logging.logger import logging
from networkSec.exception.exception import NetworkSecurityException
from networkSec.utils.main_utils.utils import save_numpy_array_data,save_object


class DataTransformationComponent:
    def __init__(self,data_transformation_config:DataTransformationConfig,data_validation_artifact:DataValidationArtifact):
        try:
           self.data_transformation_config=data_transformation_config
           self.data_validation_artifact=data_validation_artifact
        except Exception as e:
            raise NetworkSecurityException(e,sys)


    @staticmethod
    def read_data(file_path)->pd.DataFrame:
        try:
            return pd.read_csv(file_path)
        except Exception as e:
            raise NetworkSecurityException(e,sys)

    @classmethod
    def get_data_transformation_obj(cls)->Pipeline:
        logging.info("Enteretd get data trasnformation obj")
        try:
            imputer=KNNImputer(**DATA_TRANSFORMATION_IMPUTER_PARAMS)
            processor:Pipeline=Pipeline([("imputer",imputer)])

            return processor
        
        except Exception as e:
            raise NetworkSecurityException(e,sys)

    def initiate_data_transformation(self)->DataTransformationArtifact:
        try:
            logging.info("startung data transofmration")

            train_df=DataTransformationComponent.read_data(self.data_validation_artifact.valid_train_file_path)
            test_df=DataTransformationComponent.read_data(self.data_validation_artifact.valid_test_file_path)

            print(train_df.dtypes)
            print(test_df.dtypes)

            input_feature_train_df=train_df.drop(columns=[TARGET_COLUMN],axis=1)
            target_feature_train_df=train_df[TARGET_COLUMN]
            target_feature_train_df=target_feature_train_df.replace(-1,0)

            input_feature_test_df=test_df.drop(columns=[TARGET_COLUMN])
            target_feature_test_df=test_df[TARGET_COLUMN]
            target_feature_test_df=target_feature_test_df.replace(-1,0)

            print("Train input columns:", input_feature_train_df.columns.tolist())
            print("Train input types:\n", input_feature_train_df.dtypes)

            print("Test input columns:", input_feature_test_df.columns.tolist())
            print("Test input types:\n", input_feature_test_df.dtypes)


            preprocessor=self.get_data_transformation_obj()
            preprocessor_obj=preprocessor.fit(input_feature_train_df)
            transformed_input_train_feature=preprocessor_obj.transform(input_feature_train_df)
            transformed_input_test_feature=preprocessor_obj.transform(input_feature_test_df)   
            
            train_arr=np.c_[transformed_input_train_feature,np.array(target_feature_train_df)]
            test_arr=np.c_[transformed_input_test_feature,np.array(target_feature_test_df)]

            save_numpy_array_data(self.data_transformation_config.transformed_object_file_path,array=train_arr)
            save_numpy_array_data(self.data_transformation_config.transformed_test_file_path,array=test_arr)
            save_object(self.data_transformation_config.transformed_object_file_path,preprocessor_obj)

            data_transformation_artifact=DataTransformationArtifact(
                transformed_object_file_path=self.data_transformation_config,
                transformed_train_file_path=self.data_transformation_config.transformed_train_file_path,
                transformed_test_file_path=self.data_transformation_config.transformed_test_file_path
            )

            return data_transformation_artifact


        except Exception as e:
            raise NetworkSecurityException(e,sys)