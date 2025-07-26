#check if it has same schema
#check for data drift
#validate number of features 
from networkSec.entity.config_entity import DataValidationConfig
from networkSec.logging.logger import logging
from networkSec.exception.exception import NetworkSecurityException
from networkSec.entity.artifact_entity import DataIngestionArtifact, DataValidationArtifact
from networkSec.constants.training_pipeline import SCHEMA_FILE_PATH
from scipy.stats import ks_2samp
import pandas as pd 
import os,sys
from networkSec.utils.main_utils.utils import read_yaml ,write_yaml



class DataValidationComponent:
    def __init__(self,data_validation_config:DataValidationConfig,data_ingestion_artifact:DataIngestionArtifact):
        try:
            self.data_ingestion_artifact=data_ingestion_artifact
            self.data_validation_config=data_validation_config
            self._schema_config=read_yaml(SCHEMA_FILE_PATH)
            


        except Exception as e:
            raise NetworkSecurityException(e,sys)
        

    @staticmethod
    def read_data(file_path)->pd.DataFrame:
        try:
            return pd.read_csv(file_path)
        except Exception as e:
            raise NetworkSecurityException(e.sys)
   
    def validate_columns(self,dataframe:pd.DataFrame)->bool:
        try:
            number_cols=len(self._schema_config)
            logging.info(f"required number of culmns: {number_cols}")
            logging.info(f"Data Frame has columns :{len(dataframe.columns)}")

            if len(dataframe.columns)==number_cols:
                return True
            return False
        
        except Exception as e:
            raise NetworkSecurityException(e,sys)

    def data_drift(self,base_df,current_df,threshold=0.05)->bool:
        try:
            status=True
            report={}
            for col in base_df.columns:
                d1=base_df[col]
                d2=current_df[col]

                is_same=ks_2samp(d1,d2)
                if threshold<=is_same.pvalue:
                    is_found=False
                else:
                    is_found=True
                    status=False
                report.update({
                    col:{
                        "p_value":float(is_same.pvalue),
                        "drift_status":is_found
                    }
                })
            drift_report_file_path=self.data_validation_config.drift_report_file_path

            dir_path=os.path.dirname(drift_report_file_path)
            os.makedirs(dir_path,exist_ok=True)
            write_yaml(file_path=drift_report_file_path,content=report)

            return status

        except Exception as e:
            raise NetworkSecurityException(e,sys)


    def initiate_data_validation(self)->DataValidationArtifact:
        try:
            train_file_path=self.data_ingestion_artifact.trained_file_path
            test_file_path=self.data_ingestion_artifact.test_file_path

            #read data
            train_df=DataValidationComponent.read_data(train_file_path)
            test_df=DataValidationComponent.read_data(test_file_path)

            #validation
            status=self.validate_columns(dataframe=train_df)
            if not status:
                error_message=" Train dataframe does nto contain all columns"

            status=self.validate_columns(dataframe=test_df)
            if not status:
                error_message=" Test dataframe does nto contain all columns"

            ###check for numerical columns please
            

            #data drift
            status=self.data_drift(base_df=train_df,current_df=test_df)
            dir_path=os.path.dirname(self.data_validation_config.valid_train_file_path)
            os.makedirs(dir_path,exist_ok=True)

            train_df.to_csv(
                self.data_validation_config.valid_train_file_path,index=False,header=True
            )

            test_df.to_csv(
                self.data_validation_config.valid_test_file_path,index=False,header=True
            )

            data_validation_artifact=DataValidationArtifact(
                validation_status=status,
                valid_train_file_path=self.data_ingestion_artifact.trained_file_path,
                valid_test_file_path=self.data_ingestion_artifact.test_file_path,
                invalid_test_file_path=None,
                invalid_train_file_path=None,
                drift_report_file_path=self.data_validation_config.drift_report_file_path,

            )
            return data_validation_artifact




        except Exception as e:
            raise NetworkSecurityException(e,sys)

    