from networkSec.components.data_ingestion import DataIngestionComponent
from networkSec.exception.exception import NetworkSecurityException
from networkSec.logging.logger import logging
import sys 
from networkSec.entity.config_entity import DataIngestionConfig, DataValidationConfig, TrainingPipelineConfig
from networkSec.components.data_validation import DataValidationComponent


if __name__=="__main__":
    try:        
        training_pipeline_config=TrainingPipelineConfig()

        data_ingestion_config=DataIngestionConfig(training_pipeline_config)
        data_ingestion=DataIngestionComponent(data_ingestion_config=data_ingestion_config)
        logging.info("Initate the data ingestion")
        data_ingestion_artifact=data_ingestion.initiate_data_ingestion()
        logging.info("data initiating completed")
        print(data_ingestion_artifact)
        data_validation_config=DataValidationConfig(training_pipeline_config)
        data_validation=DataValidationComponent(data_validation_config=data_validation_config,data_ingestion_artifact=data_ingestion_artifact)

        logging.info("INITIATING DATA VALIDATION")

        data_validation_artifact=data_validation.initiate_data_validation()
        logging.info("DATA VALIDATION COMPLETED")
        print(data_validation_artifact)

    except Exception as e:
        raise NetworkSecurityException(e,sys)