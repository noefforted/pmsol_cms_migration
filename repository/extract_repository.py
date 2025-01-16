import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()

class ExtractRepository:
    @staticmethod
    def get_RegisterCrew():
        file_path = os.getenv("REGISTERCREW_FILE_PATH", "./data_source/RegisterCrew_202412251246.csv")
        df = pd.read_csv(file_path)
        data = df.where(pd.notnull(df), None)
        return data

    @staticmethod
    def get_City():
        file_path = os.getenv("CITY_FILE_PATH", "./data_source/CorePTKDb.City.csv")
        data = pd.read_csv(file_path)
        return data
    
    @staticmethod
    def get_Education():
        file_path = os.getenv("EDUCATION_FILE_PATH", "./data_source/RegisterCrewEducation_202412300826.csv")
        data = pd.read_csv(file_path)
        return data
    
    @staticmethod
    def get_RegisterTraining():
        file_path = os.getenv("REGISTERTRAINING_FILE_PATH", "./data_source/RegisterTraining_202501021346.csv")
        data = pd.read_csv(file_path)
        return data
    
    @staticmethod
    def get_ship():
        file_path = os.getenv("SHIP_FILE_PATH", "./data_source/CorePTKDb.Ship.csv")
        data = pd.read_csv(file_path)
        return data
    
    @staticmethod
    def get_registerDoc():
        file_path = os.getenv("REGISTERDOC_FILE_PATH", "./data_source/RegisterDoc_202501030821.csv")
        df = pd.read_csv(file_path)
        data = df.where(pd.notnull(df), None)
        return data
    
    @staticmethod
    def get_mstDoc():
        file_path = os.getenv("MSTDOC_FILE_PATH", "./data_source/MstDoc_202501061410.csv")
        data = pd.read_csv(file_path)
        return data
    
    @staticmethod
    def get_registerExperience():
        file_path = os.getenv("REGISTEREXPERIENCE_FILE_PATH", "./data_source/RegisterExperiance_202501071012.csv")
        data = pd.read_csv(file_path)
        return data
    
    @staticmethod
    def get_shipCategory():
        file_path = os.getenv("SHIPCATEGORY_FILE_PATH", "./data_source/CorePTKDb.ShipCategory.csv")
        data = pd.read_csv(file_path)
        return data
    
    @staticmethod
    def get_registerCrew_mapShipCategory():
        file_path = os.getenv("REGISTERCREW_MAPSHIPCATEGORY_FILE_PATH", "./data_source/RegisterCrew_map_shipcategory.csv")
        data = pd.read_csv(file_path)
        return data
    
    @staticmethod
    def get_mappingShipToVessel():
        file_path = os.getenv("MAPPINGSIPTOVESS_FILE_PATH", "./data_source/updated_ship_data.csv")
        data = pd.read_csv(file_path)
        return data