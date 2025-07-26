from networkSec.components.data_ingestion import DataIngestionComponent
from networkSec.exception.exception import NetworkSecurityException
from networkSec.logging.logger import logging
import sys 
from networkSec.entity.config_entity import DataIngestionConfig, TrainingPipelineConfig

if __name__=="__main__":
    try:
        training_pipeline_config=TrainingPipelineConfig()
        data_ingestion_config=DataIngestionConfig(training_pipeline_config)
        data_ingestion=DataIngestionComponent(data_ingestion_config)

        logging.info("INITIATING DATA INGESTION")

        data_ingestion_artifact=data_ingestion.initiate_data_ingestion()
        print(data_ingestion_artifact)

    except Exception as e:
        raise NetworkSecurityException(e,sys)