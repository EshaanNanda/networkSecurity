from networkSec.components.data_ingestion import DataIngestionComponent
from networkSec.components.data_transformation import DataTransformationComponent
from networkSec.components.model_trainer import ModelTrainerComponent
from networkSec.exception.exception import NetworkSecurityException
from networkSec.logging.logger import logging
import sys 
from networkSec.entity.config_entity import DataIngestionConfig, DataTransformationConfig, DataValidationConfig, ModelTrainerConfig, TrainingPipelineConfig
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


        data_transformation_config=DataTransformationConfig(training_pipeline_config)
        data_transformation=DataTransformationComponent(data_transformation_config=data_transformation_config,data_validation_artifact=data_validation_artifact)
        logging.info("INITIATING DATA TRANSFORMATION")
        data_transformation_artifact=data_transformation.initiate_data_transformation()
        logging.info("DATA TRANSFORMATION COMPLETED")
        print(data_transformation_artifact)

        logging.info("Model Training started")
        model_trainer_config=ModelTrainerConfig(training_pipeline_config)
        model_trainer=ModelTrainerComponent(model_trainer_config=model_trainer_config,data_transformation_artifact=data_transformation_artifact)
        model_trainer_artifact=model_trainer.initiate_model_trainer()
        print(model_trainer_artifact)
        logging.info("Model Training artifact created")


    except Exception as e:
        raise NetworkSecurityException(e,sys)