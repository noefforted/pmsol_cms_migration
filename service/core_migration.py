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

    @staticmethod
    async def crewing_degree():
        prisma = Prisma()
        await prisma.connect()
        try:
            await prisma.crewing_degree.delete_many()
            await prisma.crewing_degree.create_many(data=entries.degree_entry)
            log_create.info("[Created] Data Degree")
        except Exception as e:
            log_create.error(f"Error saat memasukkan data Degree: {e}")
        finally:
            await prisma.disconnect()
        
    @staticmethod
    async def crewing_designatedPersonAshore():
        prisma = Prisma()
        await prisma.connect()
        try:
            await prisma.crewing_designatedpersonashore.delete_many()
            await prisma.crewing_designatedpersonashore.create_many(data=entries.designatedPersonAshore_entry)
            log_create.info("[Created] Data Designated Person Ashore")
        except Exception as e:
            log_create.error(f"Error saat memasukkan data Designated Person Ashore: {e}")
        finally:
            await prisma.disconnect()

    @staticmethod
    async def crewing_firstParty():
        prisma = Prisma()
        await prisma.connect()
        try:
            await prisma.crewing_firstparty.delete_many()
            await prisma.crewing_firstparty.create_many(data=entries.firstParty_entry)
            log_create.info("[Created] Data First Party")
        except Exception as e:
            log_create.error(f"Error saat memasukkan data First Party: {e}")
        finally:
            await prisma.disconnect()

    @staticmethod
    async def crewing_instituion():
        prisma = Prisma()
        await prisma.connect()
        try:
            await prisma.crewing_institution.delete_many()
            await prisma.crewing_institution.create_many(data=entries.institution_entry)
            log_create.info("[Created] Data Institution")
        except Exception as e:
            log_create.error(f"Error saat memasukkan data Institution: {e}")
        finally:
            await prisma.disconnect()