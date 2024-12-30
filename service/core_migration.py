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
    async def crewing_school():
        prisma = Prisma()
        await prisma.connect()
        try:
            final_data = await service.transform.crewing_school()
            seen = set() 
            unique_data = []
            existing_data = await prisma.crewing_school.find_many()
            existing_set = {record["SchoolName"] for record in existing_data}
            for record in final_data:
                unique_key = record["SchoolName"]
                if unique_key not in seen and unique_key not in existing_set:
                    seen.add(unique_key)
                    unique_data.append(record)

            if unique_data:
                await prisma.crewing_school.create_many(data=unique_data)
                log_migrate.info(f"[Migrated] {len(unique_data)} unique records to Data School")
            else:
                log_migrate.info("No unique records to migrate.")
        except Exception as e:
            log_migrate.error(f"Error saat memasukkan data School: {e}")
        finally:
            await prisma.disconnect()
            
    @staticmethod
    async def register_crew():
        prisma = Prisma()
        await prisma.connect()
        try:
            final_data = await service.transform.crewing_registerCrew()
            await prisma.crewing_registercrew.delete_many()
            await prisma.crewing_registercrew.create_many(data=final_data)
            log_migrate.info("[Migrated] Data Register Crew")
        except Exception as e:
            log_migrate.error(f"Error saat memasukkan data Register Crew: {e}")
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

    @staticmethod
    async def crewing_jobPositionSeaService():
        prisma = Prisma()
        await prisma.connect()
        try:
            await prisma.crewing_jobpositionseaservice.delete_many()
            await prisma.crewing_jobpositionseaservice.create_many(data=entries.jobPositionSeaService_entry)
            log_create.info("[Created] Data Job Position Sea Service")
        except Exception as e:
            log_create.error(f"Error saat memasukkan data Job Position Sea Service: {e}")
        finally:
            await prisma.disconnect()

    @staticmethod
    async def crewing_major():
        prisma = Prisma()
        await prisma.connect()
        try:
            await prisma.crewing_major.delete_many()
            await prisma.crewing_major.create_many(data=entries.major_entry)
            log_create.info("[Created] Data Major")
        except Exception as e:
            log_create.error(f"Error saat memasukkan data Major: {e}")
        finally:
            await prisma.disconnect()

    @staticmethod
    async def crewing_marineSuperintendent():
        prisma = Prisma()
        await prisma.connect()
        try:
            await prisma.crewing_marinesuperintendent.delete_many()
            await prisma.crewing_marinesuperintendent.create_many(data=entries.marineSuperintendent_entry)
            log_create.info("[Created] Data Marine Superintendent")
        except Exception as e:
            log_create.error(f"Error saat memasukkan data Marine Superintendent: {e}")
        finally:
            await prisma.disconnect()

    @staticmethod
    async def crewing_sealingOfficer():
        prisma = Prisma()
        await prisma.connect()
        try:
            await prisma.crewing_sealingofficer.delete_many()
            await prisma.crewing_sealingofficer.create_many(data=entries.sealingOfficer_entry)
            log_create.info("[Created] Data Sealing Officer")
        except Exception as e:
            log_create.error(f"Error saat memasukkan data Sealing Officer: {e}")
        finally:
            await prisma.disconnect()
    
    @staticmethod
    async def crewing_shippingArea():
        prisma = Prisma()
        await prisma.connect()
        try:
            await prisma.crewing_shippingarea.delete_many()
            await prisma.crewing_shippingarea.create_many(data=entries.shippingArea_entry)
            log_create.info("[Created] Data Shipping Area")
        except Exception as e:
            log_create.error(f"Error saat memasukkan data Shipping Area: {e}")
        finally:
            await prisma.disconnect()

    @staticmethod
    async def crewing_technicalSuperintendent():
        prisma = Prisma()
        await prisma.connect()
        try:
            await prisma.crewing_technicalsuperintendent.delete_many()
            await prisma.crewing_technicalsuperintendent.create_many(data=entries.technicalSuperintendent_entry)
            log_create.info("[Created] Data Technical Superintendent")
        except Exception as e:
            log_create.error(f"Error saat memasukkan data Technical Superintendent: {e}")
        finally:
            await prisma.disconnect()

    @staticmethod
    async def crewing_training():
        prisma = Prisma()
        await prisma.connect()
        try:
            await prisma.crewing_training.delete_many()
            await prisma.crewing_training.create_many(data=entries.training_entry)
            log_create.info("[Created] Data Training")
        except Exception as e:
            log_create.error(f"Error saat memasukkan data Training: {e}")
        finally:
            await prisma.disconnect()

    @staticmethod
    async def crewing_vendor():
        prisma = Prisma()
        await prisma.connect()
        try:
            await prisma.crewing_vendor.delete_many()
            await prisma.crewing_vendor.create_many(data=entries.vendor_entry)
            log_create.info("[Created] Data Vendor")
        except Exception as e:
            log_create.error(f"Error saat memasukkan data Vendor: {e}")
        finally:
            await prisma.disconnect()

    @staticmethod
    async def crewing_vesselType():
        prisma = Prisma()
        await prisma.connect()
        try:
            await prisma.crewing_vesseltype.delete_many()
            await prisma.crewing_vesseltype.create_many(data=entries.vesselType_entry)
            log_create.info("[Created] Data Vessel Type")
        except Exception as e:
            log_create.error(f"Error saat memasukkan data Vessel Type: {e}")
        finally:
            await prisma.disconnect()

    @staticmethod
    async def crewing_jobOpening():
        prisma = Prisma()
        await prisma.connect()
        try:
            await prisma.crewing_jobopening.delete_many()
            await prisma.crewing_jobopening.create_many(data=entries.jobOpening_entry)
            log_create.info("[Created] Data Job Opening")
        except Exception as e:
            log_create.error(f"Error saat memasukkan data Job Opening: {e}")
        finally:
            await prisma.disconnect()