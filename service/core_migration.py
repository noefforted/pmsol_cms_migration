import logging
from repository.extract_repository import ExtractRepository
from prisma import Prisma
import pandas as pd
from data_source import entries
import service.transform
from datetime import datetime

log_migrate = logging.getLogger("Log Migrate")

class Migrate:
    @staticmethod
    async def crewing_city():
        prisma = Prisma()
        await prisma.connect()
        try:
            final_data = await service.transform.crewing_city()
            await prisma.crewing_city.delete_many()
            await prisma.crewing_city.create_many(data=final_data)
            log_migrate.info("[Migrated] Data City")
        except Exception as e:
            log_migrate.error(f"Error saat memasukkan data City: {e}")
        finally:
            await prisma.disconnect()

    @staticmethod
    async def register_crew():
        prisma = Prisma()
        await prisma.connect()
        try:
            # Ambil data dari CSV
            df = await ExtractRepository.get_RegisterCrew()
            log_migrate.info("[Migrated] Data Register Crew")
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
            log_create.info("[Created] Data Bank")
        except Exception as e:
            log_create.error(f"Error saat memasukkan data Bank: {e}")
        finally:
            await prisma.disconnect()

    @staticmethod
    async def crewing_interviewAssessor():
        prisma = Prisma()
        await prisma.connect()
        try:
            await prisma.crewing_interviewassessor.delete_many()
            await prisma.crewing_interviewassessor.create_many(data=entries.interviewAssessor_entry)
            log_create.info("[Created] Data Interview Assessor")
        except Exception as e:
            log_create.error(f"Error saat memasukkan data Interview Assessor: {e}")
        finally:
            await prisma.disconnect()

    @staticmethod
    async def crewing_maritalStatus():
        prisma = Prisma()
        await prisma.connect()
        try:
            await prisma.crewing_maritalstatus.delete_many()
            await prisma.crewing_maritalstatus.create_many(data=entries.maritalStatus_entry)
            log_create.info("[Created] Data Marital Status")
        except Exception as e:
            log_create.error(f"Error saat memasukkan data Marital Status: {e}")
        finally:
            await prisma.disconnect()

    @staticmethod
    async def crewing_religion():
        prisma = Prisma()
        await prisma.connect()
        try:
            await prisma.crewing_religion.delete_many()
            await prisma.crewing_religion.create_many(data=entries.religion_entry)
            log_create.info("[Created] Data Religion")
        except Exception as e:
            log_create.error(f"Error saat memasukkan data Religion: {e}")
        finally:
            await prisma.disconnect()
