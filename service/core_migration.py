import logging
from repository.extract_repository import ExtractRepository
from prisma import Prisma
import pandas as pd
from data_source import entries
import service.transform
from datetime import datetime
from util.decorator import priority

log_migrate = logging.getLogger("Log Migrate")

class Migrate:
    @staticmethod
    def dummy():
        pass

    # @staticmethod
    # @priority(1)
    # def crewing_city():
    #     prisma = Prisma()
    #     prisma.connect()
    #     try:
    #         final_data = service.transform.crewing_city()
    #         prisma.crewing_city.delete_many()
    #         prisma.crewing_city.create_many(data=final_data)
    #         log_migrate.info("[Migrated] Data City")
    #     except Exception as e:
    #         log_migrate.error(f"Error saat memasukkan data City: {e}")
    #     finally:
    #         prisma.disconnect()

    # @staticmethod
    # @priority(2)
    # def crewing_school():
    #     prisma = Prisma()
    #     prisma.connect()
    #     try:
    #         final_data = service.transform.crewing_school()
    #         seen = set() 
    #         unique_data = []
    #         existing_data = prisma.crewing_school.find_many()
    #         existing_set = {record["SchoolName"] for record in existing_data}
    #         for record in final_data:
    #             unique_key = record["SchoolName"]
    #             if unique_key not in seen and unique_key not in existing_set:
    #                 seen.add(unique_key)
    #                 unique_data.append(record)

    #         if unique_data:
    #             prisma.crewing_school.create_many(data=unique_data)
    #             log_migrate.info(f"[Migrated] {len(unique_data)} unique records to Data School")
    #         else:
    #             log_migrate.info("No unique records to migrate.")
    #     except Exception as e:
    #         log_migrate.error(f"Error saat memasukkan data School: {e}")
    #     finally:
    #         prisma.disconnect()

    @staticmethod
    @priority(3)
    def register_crew():
        prisma = Prisma()
        prisma.connect()
        try:
            final_data = service.transform.crewing_registerCrew()
            prisma.crewing_registercrew.delete_many()

            for index, row in enumerate(final_data, start=1):
                try:
                    prisma.crewing_registercrew.create(data=row)
                    log_migrate.info(f"[Created] Data RegisterCrew baris ke-{index}")
                except Exception as item_error:
                    log_migrate.error(f"Error saat membuat data Register Crew: {item_error} | Data: {row}")
            log_migrate.info(f"[Migrated] {len(final_data)} Data Register Crew")
        except Exception as e:
            log_migrate.error(f"Error saat memasukkan data Register Crew: {e}")
        finally:
            prisma.disconnect()

    @staticmethod
    @priority(4)
    def crewing_registerCrewEducation():
        prisma = Prisma()
        prisma.connect()
        try:
            final_data = service.transform.crewing_registerCrewEducation()
            prisma.crewing_registereducation.delete_many()
            for index, row in enumerate(final_data, start=1):
                try:
                    prisma.crewing_registereducation.create(data=row)
                    log_migrate.info(f"[Created] Data RegisterCrewEducation baris ke-{index}")
                except Exception as item_error:
                    log_migrate.error(f"Error saat membuat data Register Crew Education: {item_error} | Data: {row}")
            log_migrate.info(f"[Migrated] {len(final_data)} Data Register Crew Education")
        except Exception as e:
            log_migrate.error(f"Error saat memasukkan data Register Crew Education: {e}")
        finally:
            prisma.disconnect()

log_create = logging.getLogger("Log Create")
class Create:
    @staticmethod
    def dummy():
        pass

    # @staticmethod
    # def crewing_bank():
    #     prisma = Prisma()
    #     prisma.connect()
    #     try:
    #         prisma.crewing_bank.delete_many()
    #         prisma.crewing_bank.create_many(data=entries.bank_entry)
    #         log_create.info("[Created] Data Bank")
    #     except Exception as e:
    #         log_create.error(f"Error saat memasukkan data Bank: {e}")
    #     finally:
    #         prisma.disconnect()

    # @staticmethod
    # def crewing_interviewAssessor():
    #     prisma = Prisma()
    #     prisma.connect()
    #     try:
    #         prisma.crewing_interviewassessor.delete_many()
    #         prisma.crewing_interviewassessor.create_many(data=entries.interviewAssessor_entry)
    #         log_create.info("[Created] Data Interview Assessor")
    #     except Exception as e:
    #         log_create.error(f"Error saat memasukkan data Interview Assessor: {e}")
    #     finally:
    #         prisma.disconnect()

    # @staticmethod
    # def crewing_maritalStatus():
    #     prisma = Prisma()
    #     prisma.connect()
    #     try:
    #         prisma.crewing_maritalstatus.delete_many()
    #         prisma.crewing_maritalstatus.create_many(data=entries.maritalStatus_entry)
    #         log_create.info("[Created] Data Marital Status")
    #     except Exception as e:
    #         log_create.error(f"Error saat memasukkan data Marital Status: {e}")
    #     finally:
    #         prisma.disconnect()

    # @staticmethod
    # def crewing_religion():
    #     prisma = Prisma()
    #     prisma.connect()
    #     try:
    #         prisma.crewing_religion.delete_many()
    #         prisma.crewing_religion.create_many(data=entries.religion_entry)
    #         log_create.info("[Created] Data Religion")
    #     except Exception as e:
    #         log_create.error(f"Error saat memasukkan data Religion: {e}")
    #     finally:
    #         prisma.disconnect()

    # @staticmethod
    # def crewing_degree():
    #     prisma = Prisma()
    #     prisma.connect()
    #     try:
    #         prisma.crewing_degree.delete_many()
    #         prisma.crewing_degree.create_many(data=entries.degree_entry)
    #         log_create.info("[Created] Data Degree")
    #     except Exception as e:
    #         log_create.error(f"Error saat memasukkan data Degree: {e}")
    #     finally:
    #         prisma.disconnect()
        
    # @staticmethod
    # def crewing_designatedPersonAshore():
    #     prisma = Prisma()
    #     prisma.connect()
    #     try:
    #         prisma.crewing_designatedpersonashore.delete_many()
    #         prisma.crewing_designatedpersonashore.create_many(data=entries.designatedPersonAshore_entry)
    #         log_create.info("[Created] Data Designated Person Ashore")
    #     except Exception as e:
    #         log_create.error(f"Error saat memasukkan data Designated Person Ashore: {e}")
    #     finally:
    #         prisma.disconnect()

    # @staticmethod
    # def crewing_firstParty():
    #     prisma = Prisma()
    #     prisma.connect()
    #     try:
    #         prisma.crewing_firstparty.delete_many()
    #         prisma.crewing_firstparty.create_many(data=entries.firstParty_entry)
    #         log_create.info("[Created] Data First Party")
    #     except Exception as e:
    #         log_create.error(f"Error saat memasukkan data First Party: {e}")
    #     finally:
    #         prisma.disconnect()

    # @staticmethod
    # def crewing_instituion():
    #     prisma = Prisma()
    #     prisma.connect()
    #     try:
    #         prisma.crewing_institution.delete_many()
    #         prisma.crewing_institution.create_many(data=entries.institution_entry)
    #         log_create.info("[Created] Data Institution")
    #     except Exception as e:
    #         log_create.error(f"Error saat memasukkan data Institution: {e}")
    #     finally:
    #         prisma.disconnect()

    # @staticmethod
    # def crewing_jobPositionSeaService():
    #     prisma = Prisma()
    #     prisma.connect()
    #     try:
    #         prisma.crewing_jobpositionseaservice.delete_many()
    #         prisma.crewing_jobpositionseaservice.create_many(data=entries.jobPositionSeaService_entry)
    #         log_create.info("[Created] Data Job Position Sea Service")
    #     except Exception as e:
    #         log_create.error(f"Error saat memasukkan data Job Position Sea Service: {e}")
    #     finally:
    #         prisma.disconnect()

    # @staticmethod
    # def crewing_major():
    #     prisma = Prisma()
    #     prisma.connect()
    #     try:
    #         prisma.crewing_major.delete_many()
    #         prisma.crewing_major.create_many(data=entries.major_entry)
    #         log_create.info("[Created] Data Major")
    #     except Exception as e:
    #         log_create.error(f"Error saat memasukkan data Major: {e}")
    #     finally:
    #         prisma.disconnect()

    # @staticmethod
    # def crewing_marineSuperintendent():
    #     prisma = Prisma()
    #     prisma.connect()
    #     try:
    #         prisma.crewing_marinesuperintendent.delete_many()
    #         prisma.crewing_marinesuperintendent.create_many(data=entries.marineSuperintendent_entry)
    #         log_create.info("[Created] Data Marine Superintendent")
    #     except Exception as e:
    #         log_create.error(f"Error saat memasukkan data Marine Superintendent: {e}")
    #     finally:
    #         prisma.disconnect()

    # @staticmethod
    # def crewing_sealingOfficer():
    #     prisma = Prisma()
    #     prisma.connect()
    #     try:
    #         prisma.crewing_sealingofficer.delete_many()
    #         prisma.crewing_sealingofficer.create_many(data=entries.sealingOfficer_entry)
    #         log_create.info("[Created] Data Sealing Officer")
    #     except Exception as e:
    #         log_create.error(f"Error saat memasukkan data Sealing Officer: {e}")
    #     finally:
    #         prisma.disconnect()
    
    # @staticmethod
    # def crewing_shippingArea():
    #     prisma = Prisma()
    #     prisma.connect()
    #     try:
    #         prisma.crewing_shippingarea.delete_many()
    #         prisma.crewing_shippingarea.create_many(data=entries.shippingArea_entry)
    #         log_create.info("[Created] Data Shipping Area")
    #     except Exception as e:
    #         log_create.error(f"Error saat memasukkan data Shipping Area: {e}")
    #     finally:
    #         prisma.disconnect()

    # @staticmethod
    # def crewing_technicalSuperintendent():
    #     prisma = Prisma()
    #     prisma.connect()
    #     try:
    #         prisma.crewing_technicalsuperintendent.delete_many()
    #         prisma.crewing_technicalsuperintendent.create_many(data=entries.technicalSuperintendent_entry)
    #         log_create.info("[Created] Data Technical Superintendent")
    #     except Exception as e:
    #         log_create.error(f"Error saat memasukkan data Technical Superintendent: {e}")
    #     finally:
    #         prisma.disconnect()

    # @staticmethod
    # def crewing_training():
    #     prisma = Prisma()
    #     prisma.connect()
    #     try:
    #         prisma.crewing_training.delete_many()
    #         prisma.crewing_training.create_many(data=entries.training_entry)
    #         log_create.info("[Created] Data Training")
    #     except Exception as e:
    #         log_create.error(f"Error saat memasukkan data Training: {e}")
    #     finally:
    #         prisma.disconnect()

    # @staticmethod
    # def crewing_vendor():
    #     prisma = Prisma()
    #     prisma.connect()
    #     try:
    #         prisma.crewing_vendor.delete_many()
    #         prisma.crewing_vendor.create_many(data=entries.vendor_entry)
    #         log_create.info("[Created] Data Vendor")
    #     except Exception as e:
    #         log_create.error(f"Error saat memasukkan data Vendor: {e}")
    #     finally:
    #         prisma.disconnect()

    # @staticmethod
    # def crewing_vesselType():
    #     prisma = Prisma()
    #     prisma.connect()
    #     try:
    #         prisma.crewing_vesseltype.delete_many()
    #         prisma.crewing_vesseltype.create_many(data=entries.vesselType_entry)
    #         log_create.info("[Created] Data Vessel Type")
    #     except Exception as e:
    #         log_create.error(f"Error saat memasukkan data Vessel Type: {e}")
    #     finally:
    #         prisma.disconnect()

    # @staticmethod
    # def crewing_jobOpening():
    #     prisma = Prisma()
    #     prisma.connect()
    #     try:
    #         prisma.crewing_jobopening.delete_many()
    #         prisma.crewing_jobopening.create_many(data=entries.jobOpening_entry)
    #         log_create.info("[Created] Data Job Opening")
    #     except Exception as e:
    #         log_create.error(f"Error saat memasukkan data Job Opening: {e}")
    #     finally:
    #         prisma.disconnect()