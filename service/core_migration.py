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

    # @staticmethod
    # @priority(3)
    # def register_crew():
    #     prisma = Prisma()
    #     prisma.connect()
    #     try:
    #         final_data = service.transform.crewing_registerCrew()
    #         prisma.crewing_registercrew.delete_many()

    #         for index, row in enumerate(final_data, start=1):
    #             try:
    #                 prisma.crewing_registercrew.create(data=row)
    #                 log_migrate.info(f"[Created] Data RegisterCrew baris ke-{index}")
    #             except Exception as item_error:
    #                 log_migrate.error(f"Error saat membuat data Register Crew: {item_error} | Data: {row}")
    #         log_migrate.info(f"[Migrated] {len(final_data)} Data Register Crew")
    #     except Exception as e:
    #         log_migrate.error(f"Error saat memasukkan data Register Crew: {e}")
    #     finally:
    #         prisma.disconnect()

    # @staticmethod
    # @priority(4)
    # def crewing_registerCrewEducation():
    #     prisma = Prisma()
    #     prisma.connect()
    #     try:
    #         final_data = service.transform.crewing_registerCrewEducation()
    #         prisma.crewing_registereducation.delete_many()
    #         for index, row in enumerate(final_data, start=1):
    #             try:
    #                 prisma.crewing_registereducation.create(data=row)
    #                 log_migrate.info(f"[Created] Data RegisterCrewEducation baris ke-{index}")
    #             except Exception as item_error:
    #                 log_migrate.error(f"Error saat membuat data Register Crew Education: {item_error} | Data: {row}")
    #         log_migrate.info(f"[Migrated] {len(final_data)} Data Register Crew Education")
    #     except Exception as e:
    #         log_migrate.error(f"Error saat memasukkan data Register Crew Education: {e}")
    #     finally:
    #         prisma.disconnect()

    # @staticmethod
    # @priority(5)
    # def crewing_registerFamily():
    #     prisma = Prisma()
    #     prisma.connect()
    #     try:
    #         final_data = service.transform.crewing_registerFamily()
    #         prisma.crewing_registerfamily.delete_many()
    #         for index, row in enumerate(final_data, start=1):
    #             try:
    #                 prisma.crewing_registerfamily.create(data=row)
    #                 log_migrate.info(f"[Created] Data RegisterFamily baris ke-{index}")
    #             except Exception as item_error:
    #                 log_migrate.error(f"Error saat membuat data Register Family: {item_error} | Data: {row}")
    #         log_migrate.info(f"[Migrated] {len(final_data)} Data Register Family")
    #     except Exception as e:
    #         log_migrate.error(f"Error saat memasukkan data Register Family: {e}")
    #     finally:
    #         prisma.disconnect()

    # @staticmethod
    # @priority(6)
    # def crewing_registerMedicalCertificate():
    #     prisma = Prisma()
    #     prisma.connect()
    #     try:
    #         final_data = service.transform.crewing_registerMedicalCertificate()
    #         prisma.crewing_registermedicalcertificate.delete_many()
    #         for index, row in enumerate(final_data, start=1):
    #             try:
    #                 prisma.crewing_registermedicalcertificate.create(data=row)
    #                 log_migrate.info(f"[Created] Data RegisterMedicalCertificate baris ke-{index}")
    #             except Exception as item_error:
    #                 log_migrate.error(f"Error saat membuat data Register Medical Certificate: {item_error} | Data: {row}")
    #         log_migrate.info(f"[Migrated] {len(final_data)} Data Register Medical Certificate")
    #     except Exception as e:
    #         log_migrate.error(f"Error saat memasukkan data Register Medical Certificate: {e}")
    #     finally:
    #         prisma.disconnect()

    # @staticmethod
    # @priority(7)
    # def crewing_registerSeaService():
    #     prisma = Prisma()
    #     prisma.connect()
    #     try:
    #         final_data = service.transform.crewing_registerSeaService()
    #         prisma.crewing_registerseaservice.delete_many()
    #         for index, row in enumerate(final_data, start=1):
    #             try:
    #                 prisma.crewing_registerseaservice.create(data=row)
    #                 log_migrate.info(f"[Created] Data RegisterSeaService baris ke-{index}")
    #             except Exception as item_error:
    #                 log_migrate.error(f"Error saat membuat data Register Sea Service: {item_error} | Data: {row}")
    #         log_migrate.info(f"[Migrated] {len(final_data)} Data Register Sea Service")
    #     except Exception as e:
    #         log_migrate.error(f"Error saat memasukkan data Register Sea Service: {e}")
    #     finally:
    #         prisma.disconnect()

    # @staticmethod
    # @priority(9)
    # def crewing_registerTraining():
    #     prisma = Prisma()
    #     prisma.connect()
    #     try:
    #         final_data = service.transform.crewing_registerTraining()
    #         prisma.crewing_registertraining.delete_many()
    #         for index, row in enumerate(final_data, start=1):
    #             try:
    #                 prisma.crewing_registertraining.create(data=row)
    #                 log_migrate.info(f"[Created] Data RegisterTraining baris ke-{index}")
    #             except Exception as item_error:
    #                 log_migrate.error(f"Error saat membuat data Register Training: {item_error} | Data: {row}")
    #         log_migrate.info(f"[Migrated] {len(final_data)} Data Register Training")
    #     except Exception as e:
    #         log_migrate.error(f"Error saat memasukkan data Register Training: {e}")
    #     finally:
    #         prisma.disconnect()

    # @staticmethod
    # @priority(10)
    # def crewing_registerTravelDoc():
    #     prisma = Prisma()
    #     prisma.connect()
    #     try:
    #         final_data = service.transform.crewing_registerTravelDoc()
    #         prisma.crewing_registertraveldoc.delete_many()
    #         for index, row in enumerate(final_data, start=1):
    #             try:
    #                 prisma.crewing_registertraveldoc.create(data=row)
    #                 log_migrate.info(f"[Created] Data RegisterTravelDoc baris ke-{index}")
    #             except Exception as item_error:
    #                 log_migrate.error(f"Error saat membuat data Register Travel Doc: {item_error} | Data: {row}")
    #         log_migrate.info(f"[Migrated] {len(final_data)} Data Register Travel Doc")
    #     except Exception as e:
    #         log_migrate.error(f"Error saat memasukkan data Register Travel Doc: {e}")
    #     finally:
    #         prisma.disconnect()

    # @staticmethod
    # @priority(11)
    # def crewing_assessmentHistory():
    #     prisma = Prisma()
    #     prisma.connect()
    #     try:
    #         final_data = service.transform.crewing_assessmentHistory()
    #         prisma.crewing_assessmenthistory.delete_many()
    #         for index, row in enumerate(final_data, start=1):
    #             try:
    #                 prisma.crewing_assessmenthistory.create(data=row)
    #                 log_migrate.info(f"[Created] Data AssessmentHistory baris ke-{index}")
    #             except Exception as item_error:
    #                 log_migrate.error(f"Error saat membuat data Assessment History: {item_error} | Data: {row}")
    #         log_migrate.info(f"[Migrated] {len(final_data)} Data Assessment History")
    #     except Exception as e:
    #         log_migrate.error(f"Error saat memasukkan data Assessment History: {e}")
    #     finally:
    #         prisma.disconnect()

    # @staticmethod
    # @priority(8)
    # def crewing_vessel():
    #     prisma = Prisma()
    #     prisma.connect()
    #     try:
    #         final_data = service.transform.crewing_vessel()
    #         # prisma.crewing_vessel.delete_many()
    #         for index, row in enumerate(final_data, start=1):
    #             try:
    #                 prisma.crewing_vessel.create(data=row)
    #                 log_migrate.info(f"[Created] Data vessel baris ke-{index}")
    #             except Exception as item_error:
    #                 log_migrate.error(f"Error saat membuat data vessel: {item_error} | Data: {row}")
    #         log_migrate.info(f"[Migrated] {len(final_data)} Data vessel")
    #     except Exception as e:
    #         log_migrate.error(f"Error saat memasukkan data vessel: {e}")
    #     finally:
    #         prisma.disconnect()

    # @staticmethod
    # @priority(12)
    # def crewing_employee():
    #     prisma = Prisma()
    #     prisma.connect()
    #     try:
    #         final_data = service.transform.crewing_employee()
    #         # prisma.crewing_employee.delete_many()
    #         for index, row in enumerate(final_data, start=1):
    #             try:
    #                 # prisma.crewing_employee.create(data=row)
    #                 prisma.crewing_employee.update(
    #                     where={"Id": row["Id"]},
    #                     data={"PhoneNo": row["PhoneNo"]}
    #                 )
    #                 log_migrate.info(f"[Updated] Data Employee baris ke-{index}")
    #             except Exception as item_error:
    #                 log_migrate.error(f"Error saat membuat data Employee: {item_error} | Data: {row}")
    #         log_migrate.info(f"[Migrated] {len(final_data)} Data Employee")
    #     except Exception as e:
    #         log_migrate.error(f"Error saat memasukkan data Employee: {e}")
    #     finally:
    #         prisma.disconnect()

    # @staticmethod
    # @priority(13)
    # def crewing_employeeCOCDoc():
    #     prisma = Prisma()
    #     prisma.connect()
    #     try:
    #         final_data = service.transform.crewing_employeeCOCDoc()
    #         prisma.crewing_employeecocdoc.delete_many()
    #         for index, row in enumerate(final_data, start=1):
    #             try:
    #                 # print(row)
    #                 prisma.crewing_employeecocdoc.create(data=row)
    #                 log_migrate.info(f"[Created] Data Employee COC Doc baris ke-{index}")
    #             except Exception as e:
    #                 log_migrate.error(f"Error saat membuat data Employee COC Doc: {e}")
    #                 continue
    #         log_migrate.info(f"[Migrated] {len(final_data)} Data Employee COC Doc")
    #     except Exception as e:
    #         log_migrate.error(f"Error saat memasukkan data Employee COC Doc: {e}")
    #     finally:
    #         prisma.disconnect()

    @staticmethod
    @priority(14)
    def crewing_employeeCOPDoc():
        prisma = Prisma()
        prisma.connect()
        try:
            final_data = service.transform.crewing_employeeCOPDoc()
            prisma.crewing_employeecopdoc.delete_many()
            for index, row in enumerate(final_data, start=1):
                try:
                    prisma.crewing_employeecopdoc.create(data=row)
                    log_migrate.info(f"[Created] Data Employee COP Doc baris ke-{index}")
                except Exception as e:
                    log_migrate.error(f"Error saat membuat data Employee COP Doc: {e}")
                    continue
            log_migrate.info(f"[Migrated] {len(final_data)} Data Employee COP Doc")
        except Exception as e:
            log_migrate.error(f"Error saat memasukkan data Employee COP Doc: {e}")
        finally:
            prisma.disconnect()

    # @staticmethod
    # @priority(15)
    # def crewing_employeeEducation():
    #     prisma = Prisma()
    #     prisma.connect()
    #     try:
    #         final_data = service.transform.crewing_employeeEducation()
    #         prisma.crewing_employeeeducation.delete_many()
    #         for index, row in enumerate(final_data, start=1):
    #             try:
    #                 prisma.crewing_employeeeducation.create(data=row)
    #                 log_migrate.info(f"[Created] Data Employee Education baris ke-{index}")
    #             except Exception as item_error:
    #                 log_migrate.error(f"Error saat membuat data Employee Education: {item_error} | Data: {row}")
    #         log_migrate.info(f"[Migrated] {len(final_data)} Data Employee Education")
    #     except Exception as e:
    #         log_migrate.error(f"Error saat memasukkan data Employee Education: {e}")
    #     finally:
    #         prisma.disconnect()

    # @staticmethod
    # @priority(16)
    # def crewing_employeeFamily():
    #     prisma = Prisma()
    #     prisma.connect()
    #     try:
    #         final_data = service.transform.crewing_employeeFamily()
    #         prisma.crewing_employeefamily.delete_many()
    #         for index, row in enumerate(final_data, start=1):
    #             try:
    #                 prisma.crewing_employeefamily.create(data=row)
    #                 log_migrate.info(f"[Created] Data Employee Family baris ke-{index}")
    #             except Exception as item_error:
    #                 log_migrate.error(f"Error saat membuat data Employee Family: {item_error} | Data: {row}")
    #         log_migrate.info(f"[Migrated] {len(final_data)} Data Employee Family")
    #     except Exception as e:
    #         log_migrate.error(f"Error saat memasukkan data Employee Family: {e}")
    #     finally:
    #         prisma.disconnect()

    # @staticmethod
    # @priority(17) 
    # def crewing_employeeMedicalCertificate():
    #     prisma = Prisma()
    #     prisma.connect()
    #     try:
    #         final_data = service.transform.crewing_employeeMedicalCertificate()
    #         prisma.crewing_employeemedicalcertificate.delete_many()
    #         for index, row in enumerate(final_data, start=1):
    #             try:
    #                 prisma.crewing_employeemedicalcertificate.create(data=row)
    #                 log_migrate.info(f"[Created] Data Employee Medical Certificate baris ke-{index}")
    #             except Exception as item_error:
    #                 log_migrate.error(f"Error saat membuat data Employee Medical Certificate: {item_error} | Data: {row}")
    #         log_migrate.info(f"[Migrated] {len(final_data)} Data Employee Medical Certificate")
    #     except Exception as e:
    #         log_migrate.error(f"Error saat memasukkan data Employee Medical Certificate: {e}")
    #     finally:
    #         prisma.disconnect()

    # @staticmethod
    # @priority(19)
    # def crewing_employeeTraining():
    #     prisma = Prisma()
    #     prisma.connect()
    #     try:
    #         final_data = service.transform.crewing_employeeTraining()

    #         # Ambil semua EmployeeId dari tabel parent untuk validasi
    #         valid_employee_ids = {employee.Id for employee in prisma.crewing_employee.find_many()}
    #         # print(f"Valid EmployeeIds: {valid_employee_ids}, Len = {len(valid_employee_ids)}")

    #         prisma.crewing_employeetraining.delete_many()
    #         for index, row in enumerate(final_data, start=1):
    #             try:
    #                 row["EmployeeId"] = row["EmployeeId"].lower()
    #                 if row["EmployeeId"] not in valid_employee_ids:
    #                     log_migrate.warning(f"[Skipped] Data EmployeeId '{row['EmployeeId']}' tidak valid.")
    #                     continue

    #                 prisma.crewing_employeetraining.create(data=row)
    #                 log_migrate.info(f"[Created] Data Employee Training baris ke-{index}")
    #             except Exception as item_error:
    #                 log_migrate.error(f"Error saat membuat data Employee Training: {item_error} | Data: {row}")
    #         log_migrate.info(f"[Migrated] {len(final_data)} Data Employee Training")
    #     except Exception as e:
    #         log_migrate.error(f"Error saat memasukkan data Employee Training: {e}")
    #     finally:
    #         prisma.disconnect()

    # @staticmethod
    # @priority(20)
    # def crewing_employeeSeaService():
    #     prisma = Prisma()
    #     prisma.connect()
    #     try:
    #         final_data = service.transform.crewing_employeeSeaService()
    #         valid_employee_ids = {employee.Id for employee in prisma.crewing_employee.find_many()}
    #         prisma.crewing_employeeseaservice.delete_many()
    #         for index, row in enumerate(final_data, start=1):
    #             try:
    #                 row["EmployeeId"] = row["EmployeeId"].lower()
    #                 if row["EmployeeId"] not in valid_employee_ids:
    #                     log_migrate.warning(f"[Skipped] Data EmployeeId '{row['EmployeeId']}' tidak valid.")
    #                     continue
    #                 prisma.crewing_employeeseaservice.create(data=row)
    #                 log_migrate.info(f"[Created] Data Employee Sea Service baris ke-{index}")
    #             except Exception as item_error:
    #                 log_migrate.error(f"Error saat membuat data Employee Sea Service: {item_error} | Data: {row}")
    #         log_migrate.info(f"[Migrated] {len(final_data)} Data Employee Sea Service")
    #     except Exception as e:
    #         log_migrate.error(f"Error saat memasukkan data Employee Sea Service: {e}")
    #     finally:
    #         prisma.disconnect()

    # @staticmethod
    # @priority(21)
    # def crewing_employeeTravelDoc():
    #     prisma = Prisma()
    #     prisma.connect()
    #     try:
    #         final_data = service.transform.crewing_employeeTravelDoc()
    #         prisma.crewing_employeetraveldoc.delete_many()
    #         for index, row in enumerate(final_data, start=1):
    #             try:
    #                 prisma.crewing_employeetraveldoc.create(data=row)
    #                 log_migrate.info(f"[Created] Data Employee Travel Doc baris ke-{index}")
    #             except Exception as item_error:
    #                 log_migrate.error(f"Error saat membuat data Employee Travel Doc: {item_error} | Data: {row}")
    #         log_migrate.info(f"[Migrated] {len(final_data)} Data Employee Travel Doc")
    #     except Exception as e:
    #         log_migrate.error(f"Error saat memasukkan data Employee Travel Doc: {e}")
    #     finally:
    #         prisma.disconnect()

    # @staticmethod
    # @priority(22)
    # def update_employeeShipId():
    #     prisma = Prisma()
    #     prisma.connect()
    #     try:
    #         final_data = service.transform.update_employeeShipId()
    #         for index, row in enumerate(final_data, start=1):
    #             try:
    #                 prisma.crewing_employee.update(
    #                     where={"Id": row["Id"]},  # Gunakan kunci unik dari tabel Anda
    #                     data={"ShipId": row["ShipId"]}  # Data yang diperbarui
    #                 )
    #                 log_migrate.info(f"[Updated] Data Employee ShipId baris ke-{index}")
    #             except Exception as item_error:
    #                 log_migrate.error(f"Error saat memperbarui data Employee ShipId: {item_error} | Data: {row}")
    #     except Exception as e:
    #         log_migrate.error(f"Error saat memperbarui data Employee ShipId: {e}")
    #     finally:
    #         prisma.disconnect()

    # @staticmethod
    # @priority(23)
    # def crewing_employeeBlacklist():
    #     prisma = Prisma()
    #     prisma.connect()
    #     try:
    #         final_data = service.transform.crewing_employeeBlacklist()
    #         # print(f"Final Data: {final_data}")
    #         prisma.crewing_employeeblacklist.delete_many()
    #         for index, row in enumerate(final_data, start=1):
    #             try:
    #                 prisma.crewing_employeeblacklist.create(data=row)
    #                 log_migrate.info(f"[Created] Data Employee Blacklist baris ke-{index}")
    #             except Exception as item_error:
    #                 log_migrate.error(f"Error saat membuat data Employee Blacklist: {item_error} | Data: {row}")
    #         log_migrate.info(f"[Migrated] {len(final_data)} Data Employee Blacklist")
    #     except Exception as e:
    #         log_migrate.error(f"Error saat memasukkan data Employee Blacklist: {e}")
    #     finally:
    #         prisma.disconnect()
    
    # @staticmethod
    # @priority(24)
    # def crewing_employeeBoardSchedule():
    #     prisma = Prisma()
    #     prisma.connect()
    #     try:
    #         final_data = service.transform.crewing_employeeBoardSchedule()
    #         prisma.crewing_employeeboardschedule.delete_many()
    #         for index, row in enumerate(final_data, start=1):
    #             try:
    #                 prisma.crewing_employeeboardschedule.create(data=row)
    #                 log_migrate.info(f"[Created] Data Employee Board Schedule baris ke-{index}")
    #             except Exception as item_error:
    #                 log_migrate.error(f"Error saat membuat data Employee Board Schedule: {item_error} | Data: {row}")
    #         log_migrate.info(f"[Migrated] {len(final_data)} Data Employee Board Schedule")
    #     except Exception as e:
    #         log_migrate.error(f"Error saat memasukkan data Employee Board Schedule: {e}")
    #     finally:
    #         prisma.disconnect()

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