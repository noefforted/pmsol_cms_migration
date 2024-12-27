import logging
from repository.extract_repository import ExtractRepository
from prisma import Prisma
import pandas as pd
from data_source import entries
from datetime import datetime
import uuid

log_migrate = logging.getLogger("Log Migraste")

class Migrate:
    @staticmethod
    async def crewing_city():
        prisma = Prisma()
        await prisma.connect()
        try:
            df = pd.read_csv('data_source/CorePTKDb.City.csv')
            final_data = [
                {
                    "Id": str(uuid.uuid4()),
                    "CityName": row["Name"],
                    "Created": datetime.now(),
                    "CreatedBy": "Migration",
                    "IsDeleted": False,
                    "Modified": datetime.now(),
                    "ModifiedBy": None
                }
                for _, row in df.iterrows()
            ]
            await prisma.crewing_city.delete_many()
            await prisma.crewing_city.create_many(data=final_data)
            log_migrate.info("Data City berhasil dimasukkan")
        finally:
            await prisma.disconnect()

    @staticmethod
    async def register_crew():
        prisma = Prisma()
        await prisma.connect()
        try:
            # Ambil data dari CSV
            df = await ExtractRepository.get_RegisterCrew()
            log_migrate.info("Mengambil data dari RegisterCrew")
            dt_array = df.to_numpy()


        finally:
            await prisma.disconnect()


log_create = logging.getLogger("Log Create")
class Create:
    @staticmethod
    async def crewing_bank():
        prisma = Prisma()
        await prisma.connect()
        try:
            await prisma.crewing_bank.delete_many()
            await prisma.crewing_bank.create_many(data=entries.bank_entry)
            log_create.info("Data Bank berhasil dimasukkan")
        finally:
            await prisma.disconnect()

    @staticmethod
    async def crewing_interviewAssessor():
        prisma = Prisma()
        await prisma.connect()
        try:
            await prisma.crewing_interviewassessor.delete_many()
            await prisma.crewing_interviewassessor.create_many(data=entries.interviewAssessor_entry)
            log_create.info("Data Interview Assessor berhasil dimasukkan")
        finally:
            await prisma.disconnect()

    @staticmethod
    async def crewing_maritalStatus():
        prisma = Prisma()
        await prisma.connect()
        try:
            await prisma.crewing_maritalstatus.delete_many()
            await prisma.crewing_maritalstatus.create_many(data=entries.maritalStatus_entry)
            log_create.info("Data Marital Status berhasil dimasukkan")
        finally:
            await prisma.disconnect()

    @staticmethod
    async def crewing_religion():
        prisma = Prisma()
        await prisma.connect()
        try:
            await prisma.crewing_religion.delete_many()
            await prisma.crewing_religion.create_many(data=entries.religion_entry)
            log_create.info("Data Religion berhasil dimasukkan")
        finally:
            await prisma.disconnect()
