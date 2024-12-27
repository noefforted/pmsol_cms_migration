import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()


class ExtractRepository:
    @staticmethod
    async def get_RegisterCrew():
        file_path = os.getenv("REGISTERCREW_FILE_PATH",
                              "./data_source/RegisterCrew_202412251246.csv")
        data = pd.read_csv(file_path)
        return data
