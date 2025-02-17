import uuid
from datetime import datetime
from repository.extract_repository import ExtractRepository
from repository.log_repository import LogRepository
import pandas as pd
import logging
import random
from datetime import datetime

log_transform = logging.getLogger("Log Transform")


def crewing_city():
    df = ExtractRepository.get_City()
    data = [
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
    return data

def crewing_school():
    df = ExtractRepository.get_Education()
    data = [
        {
            "Id": row["Id"],
            "SchoolName": row["SchoolName"],
            "Created": datetime.now(),
            "CreatedBy": "Migration",
            "IsDeleted": False,
            "Modified": datetime.now(),
            "ModifiedBy": None
        }
        for _, row in df.iterrows()
    ]
    return data

# Fungsi untuk konversi ke ISO-8601
def convert_to_iso(date_str):
    try:
        # Mengubah format tanggal ke ISO-8601 dengan zona waktu Z
        return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S.%f").isoformat() + "Z"
    except ValueError:
        return None 

bank_id_to_uuid_mapping = {
    1: "95900ADD-5CD7-41DC-9750-86D674694B77",  # Bank Central Asia
    2: "3B9DF32A-AEE7-432C-9D4E-24AE970D64E2",  # Bank Mandiri
    3: "AB4B0FD8-BE96-48D7-8688-3EE9CB7A32BD",  # Bank Mandiri Syariah
    4: "745E032D-27C6-43CD-BCC4-0EEAEEFCC277",  # -
    5: "68BD012C-49E7-48F5-B578-4C19A916DB56",  # BANK CIMB NIAGA
    6: "4ED6D9F7-9B86-401C-BD15-64315043FD0E",  # BANK BRI
    7: "412E9296-89A6-4657-99D2-27D322457E3D",  # BANK BNI
    8: "9BE2C923-4F75-4E13-A46E-9E823D2CC04C",  # Panin Bank
    9: "CC15E7D2-F27C-48C6-81E8-5DF9C15C538E",  # BANK BNI SYARIAH
    10: "F772C68A-1B08-4D3D-A776-0145115C14D5",  # Bank Muamalat
    11: "FE29AD6E-B991-4233-8C36-44F820BB2622",  # BANK BRI SYARIAH
    12: "8A0989CE-0E7C-4E6E-B701-5095E807A2EB",  # MAYBANK
    13: "FF0AB086-9C9D-4096-9699-0B00BB4C39C3",  # Bank BTN
}

religion_id_to_uuid_mapping = {
    1: "6CB2019D-2B83-40C9-A8BC-5C25102309B5",  # Islam
    2: "CE00FE54-B10D-47EF-8B8E-A2E855F712A6",  # Katolik
    3: "D2E6B209-65C8-42E8-B5F5-808F4D9E6166",  # Protestan
    4: "5A83557C-859A-483B-8BF1-43706401DA89",  # Buddha
    5: "67502D36-BC9E-46D9-8111-17745C885B29",  # Hindu
    6: "06C27876-50A6-436F-8FDF-1EF0EA6AEB96",  # Konghuchu
}

marital_status_id_to_uuid_mapping = {
    1: "6EEBD3CE-9FAD-4566-ABD9-197416643170",  # Single
    2: "8A821D53-82CA-4DEA-A575-694AEC168322",  # Married
    3: "7DF630F8-CCCE-42BE-BD00-2617F4109B0A",  # Divorced
    4: "3930B8E2-CEA5-444F-904B-FC8A0D3857C1",  # Widowed
    5: "4434851E-BA67-4939-8FDD-8611002E56C2"  # Death Divorced
}

isDeleted_mapping = {
    "N": False,
    "Y": True
}

def crewing_registerCrew():
    df = ExtractRepository.get_RegisterCrew()
    # df = df.iloc[:10]
    data = []
    for i, row in df.iterrows():
        try:
            transformed_data = {
                "Id": row["Id"],
                "RegCrewNo": LogRepository.get_last_regCrewNo() + i + 1,
                "RegRegisNo": row["NIP"] if row["NIP"] else None,
                "RegFullName": " ".join(filter(None, [row.get('FirstName'), row.get('LastName')])).strip() 
                if row.get('FirstName') or row.get('LastName') else "Unknown",
                "RegAddress": row["Address"] if row["Address"] else None,
                "RegEmail": row["Email"] if row["Email"] else None,
                "RegGender": row["Gender"] if row["Gender"] else None,
                "RegPlaceOfBirth": row["BirthLoc"] if row["BirthLoc"] else None,
                "RegDateOfBirth": convert_to_iso(row["BirthDate"]) if row["BirthDate"] else None,
                "RegReligionId": religion_id_to_uuid_mapping.get(row["ReligionId"], "6CB2019D-2B83-40C9-A8BC-5C25102309B5"),
                "RegMaritalStatusId": marital_status_id_to_uuid_mapping.get(row["MaritalStatusId"], "6EEBD3CE-9FAD-4566-ABD9-197416643170"),
                "RegNationality": "Indonesia",
                "RegPhotoId": None,
                "RegPassportPhotoId": None,
                "RegKtpPhotoId": None,
                "RegIdCardNo": row["IdentityCardNo"] if row["IdentityCardNo"] else None,
                "RegIdPassport": row["PasporBook"] if row["PasporBook"] else None,
                "RegFamilyCardNo": row["KKNumber"] if row["KKNumber"] else None,
                "RegNPWP": row["NPWP"].replace('.', '') if row["NPWP"] else None,
                "RegBPJSKesNo": row["BPJSKesNumber"] if row["BPJSKesNumber"] else None,
                "RegBPJSTKNo": row["BPJSKetNumber"] if row["BPJSKetNumber"] else None,
                "RegPhoneNo": str(row["CellPhone"]).strip() if pd.notnull(row["CellPhone"]) else None,
                "RegRelativesName": None,
                "RegRelativesEmail": None,
                "RegRelativesPhone": None,
                "RegRelativesAddress": None,
                "RegBankAccount1": row["BankAccountNo"] if row["BankAccountNo"] else None,
                "RegBankId1": bank_id_to_uuid_mapping.get(row["BankId"], "4ED6D9F7-9B86-401C-BD15-64315043FD0E"),
                "RegBankAccountName1": row["BankAccountName"] if row["BankAccountName"] else None,
                "RegBankAccount2": row["BankAccountNo2"] if row["BankAccountNo2"] else None,
                "RegBankId2": bank_id_to_uuid_mapping.get(row["BankId2"], "4ED6D9F7-9B86-401C-BD15-64315043FD0E"),
                "RegBankAccountName2": row["BankAccountName2"] if row["BankAccountName2"] else None,
                "RegDivisionId": 1, #dummy
                "CurrentStatusId": 1, #dummy
                "JobOpeningId": "2184F223-4B8D-4C1A-9542-3C3DF101492B",
                "StatusUrl": None,
                "RegApplyDate": convert_to_iso(row["CreatedAt"]),
                "RegSeaFarerId": None,
                "EmployeeIdNumber": None,
                "WilingToAcceptLowerRank": None,
                "AvailableForm": None,
                "AvailableUntil": None,
                "Weight": None,
                "Height": None,
                "BMI": None,
                "SafetyShoesSize": None,
                "TrousersSize": None,
                "CoverallSize": None,
                "ShirtSize": None,
                "Created": datetime.now(),
                "CreatedBy": "Migration",
                "IsDeleted": isDeleted_mapping.get(row["IsDeleted"], False),
                "Modified": datetime.now(),
                "ModifiedBy": None,
                "RegSeamanBookNo": None,
                "TemporaryPassword": None
            }
            log_transform.info(f"[Transformed] RegisterCrew Data baris ke-{i+1}")
            data.append(transformed_data)
        except Exception as transform_error:
            log_transform.error(f"Error saat transformasi RegisterCrew data baris ke-{i+1}: {transform_error}")
    return data

def convert_to_int(year_value):
    try:
        # Jika nilai adalah None atau NaN, kembalikan None
        if pd.isnull(year_value):
            return None
        # Jika nilai adalah float atau int, konversi langsung ke int
        if isinstance(year_value, (float, int)):
            return int(year_value)
        # Jika nilai adalah string, cek apakah string mengandung angka
        return int(year_value[:4]) if year_value and year_value.isdigit() else None
    except (ValueError, TypeError):
        return None  # Jika ada error, kembalikan None

def crewing_registerCrewEducation():
    log_transform = logging.getLogger("Log Transform Education")

    # Fetch data secara asinkron
    df = ExtractRepository.get_Education()
    city = ExtractRepository.get_City()

    # Buat dictionary lookup untuk city
    city_lookup = {row["Id"]: row["Name"] for _, row in city.iterrows()}

    # Buat list data
    data = []
    for i, row in df.iterrows():
        try:
            transformed_data = {
                "Id": row["Id"],
                "RegisterCrewId": row["CrewID"] if row["CrewID"] else None,
                "RegEduSchool": row["SchoolName"] if row["SchoolName"] else None,
                "RegEduDegree": row["Degree"] if row["Degree"] else None,
                "RegEduMajor": None if pd.isnull(row["Prodi"]) else row["Prodi"],
                "RegEduGPA": row["Score"] if row["Score"] else None,
                "RegEduYearAdmission": convert_to_int(row["EntryYear"]),
                "RegEduYearGraduate": convert_to_int(row["LeaveYear"]),
                "RegEduCity": city_lookup.get(row["City"], "Unknown"),  # Lookup city name atau gunakan default
                "Created": datetime.now(),
                "CreatedBy": "Migration",
                "IsDeleted": isDeleted_mapping.get(row["IsDeleted"], False),
                "Modified": datetime.now(),
                "ModifiedBy": None
            }
            log_transform.info(f"[Transformed] Data registerCrewEducation baris ke-{i+1}")
            data.append(transformed_data)
        except Exception as transform_error:
            log_transform.error(f"Error saat transformasi registerCrewEducation data baris ke-{i+1}: {transform_error}")
    return data

def crewing_registerFamily():
    df = ExtractRepository.get_RegisterCrew()
    data = []
    for i, row in df.iterrows():
        try:
            transformed_data = {
                "Id": str(uuid.uuid4()),
                "RegisterCrewId": row["Id"],
                "Relationship": None,
                "Fullname": None,
                "Gender": None,
                "DateOfBirth": None,
                "MobileNo": None,
                "Email": None,
                "KtpNumber": None,
                "FamilyRegisterNumber": None,
                "Address": None,
                "Created": datetime.now(),
                "CreatedBy": "Migration",
                "Modified": None,
                "ModifiedBy": None,
                "IsDeleted": False
            }
            log_transform.info(f"[Transformed] Data registerFamily baris ke-{i+1}")
            data.append(transformed_data)
        except Exception as transform_error:
            log_transform.error(f"Error saat transformasi registerFamily data baris ke-{i+1}: {transform_error}")
    return data

def crewing_registerMedicalCertificate():
    try:
        # Fetching data from repositories
        df = ExtractRepository.get_RegisterCrew()
        data_cE = LogRepository.get_crewing_registerCrew()
        data_cE = pd.DataFrame([dict(item) for item in data_cE])  # Convert to DataFrame

        data = []
        for i, row in df.iterrows():
            try:
                # Filter to find the corresponding RegPhotoId
                attachment_file_id = data_cE.loc[data_cE["Id"] == row["Id"], "RegPhotoId"].iloc[0] if not data_cE.loc[data_cE["Id"] == row["Id"]].empty else None
                
                # Prepare transformed data
                transformed_data = {
                    "Id": str(uuid.uuid4()),
                    "RegisterCrewId": row["Id"],
                    "MedicalCertificateName": "MCU",
                    "Number": None,
                    "PlaceOfIssue": None,
                    "DateOfIssue": None,
                    "ExpirationDate": None,
                    "AttachmentFileId": attachment_file_id,
                    "Created": datetime.now(),
                    "CreatedBy": "Migration",
                    "Modified": None,
                    "ModifiedBy": None,
                    "IsDeleted": False
                }
                
                log_transform.info(f"[Transformed] Data registerMedicalCertificate row {i + 1} transformed successfully.")
                data.append(transformed_data)

            except Exception as transform_error:
                log_transform.error(f"Error transforming row {i + 1}: {transform_error}")

        return data

    except Exception as e:
        log_transform.error(f"Critical error in crewing_registerMedicalCertificate: {e}")
        return []
    
def crewing_registerSeaService():
    # Mengambil data dari repository
    df = ExtractRepository.get_RegisterCrew()
    data = []
    
    for i, row in df.iterrows():
        try:
            # Data transformasi sesuai dengan kolom di tabel Crewing_RegisterSeaService
            transformed_data = {
                "Id": str(uuid.uuid4()),
                "RegisterCrewId": row["Id"],
                "RegSeaServiceCompanyName": None,  # Nama Perusahaan
                "RegSeaServicePosition": None,  # Jabatan/Posisi
                "RegSeaServiceShipName": None,  # Nama Kapal
                "RegSeaServiceShipType": None,  # Tipe Kapal
                "RegSeaServiceGRT": None,  # Gross Tonnage
                "RegSeaServiceOnBoard": None,  # Tanggal On Board
                "RegSeaServiceOffBoard": None,  # Tanggal Off Board
                "RegSeaServiceNote": None,  # Catatan tambahan
                "RegSeaServiceAttachment": None,  # File Attachment
                "Created": datetime.now(),
                "CreatedBy": "Migration",
                "Modified": None,
                "ModifiedBy": None,
                "IsDeleted": False
            }
            
            # Logging berhasil transformasi
            log_transform.info(f"[Transformed] Data registerSeaService baris ke-{i+1} berhasil ditransformasi.")
            data.append(transformed_data)

        except Exception as transform_error:
            # Logging error per baris
            log_transform.error(f"Error saat transformasi registerSeaService data baris ke-{i+1}: {transform_error}")

    return data

def crewing_registerTraining():
    df = ExtractRepository.get_RegisterTraining()
    data = []
    
    for i, row in df.iterrows():
        try:
            # Data transformasi sesuai dengan kolom di tabel Crewing_RegisterTraining
            transformed_data = {
                "Id": str(uuid.uuid4()),
                "RegisterCrewId": row["CrewId"],
                "RegTrainingInstitution": row["Institution"] if row["Institution"] else None,
                "RegTrainingPosition": row["TrainingName"] if row["TrainingName"] else None,
                "RegTrainingCategory": str(row["TrainingCategoryId"]) if row["TrainingCategoryId"] else None,
                "RegTrainingInstructor": row["InstructorName"] if row["InstructorName"] else None,  
                "RegTrainingFeedback": row["TrainingFeedBack"] if row["TrainingFeedBack"] else None,
                "RegTrainingEval": row["Evaluation"] if row["Evaluation"] else None,
                "RegTrainingStartDate": convert_to_iso(row["StartDate"]) if row["StartDate"] else None,  # Tanggal mulai training
                "RegTrainingEndDate": convert_to_iso(row["EndDate"]) if row["EndDate"] else None,  # Tanggal selesai training
                "Created": datetime.now(),
                "CreatedBy": "Migration",
                "Modified": None,
                "ModifiedBy": None,
                "IsDeleted": False
            }
            
            # Logging berhasil transformasi
            log_transform.info(f"[Transformed] Data registerTraining baris ke-{i+1} berhasil ditransformasi.")
            data.append(transformed_data)

        except Exception as transform_error:
            # Logging error per baris
            log_transform.error(f"Error saat transformasi registerTraining data baris ke-{i+1}: {transform_error}")

    return data

def crewing_registerTravelDoc():
    df = ExtractRepository.get_RegisterCrew()
    data = []
    for i, row in df.iterrows():
        try:
            transformed_data = {
                "Id": str(uuid.uuid4()),
                "RegisterCrewId": row["Id"],
                "RegTravelDocName": None,
                "RegTravelDocNo": None,
                "RegDocDateIssued": None, 
                "RegDocDateExpired": None,
                "RegTravelInformation": None,
                "Created": datetime.now(),
                "CreatedBy": "Migration",
                "Modified": None, 
                "ModifiedBy": None,
                "IsDeleted": False
            }
            log_transform.info(f"[Transformed] Data registerTraverDoc baris ke-{i+1}")
            data.append(transformed_data)
        except Exception as transform_error:
            log_transform.error(f"Error saat transformasi registerTraverDoc data baris ke-{i+1}: {transform_error}")
    return data

def crewing_assessmentHistory():
    # Mengambil data dari repository
    df = ExtractRepository.get_RegisterCrew()
    data = []
    assesment_types = ["Screening", "BOC", "CES", "MARLIN", "Interview", "MCU", "Result", "Fail"]
    assesment_status = ["Pending", "InProgress", "Passed", "Failed"]
    for i, row in df.iterrows():
        try:
            # Data transformasi sesuai dengan kolom di tabel Crewing_AssessmentHistory
            transformed_data = {
                "Id": str(uuid.uuid4()),
                "AssessmentType": random.choice(assesment_types), 
                "Schedule": None,
                "Location": None,
                "AssessmentStatus": random.choice(assesment_status),
                "Score": None,
                "Note": None,
                "BOCResultStatus": None,
                "BOCDocument": None,
                "ClinicName": None,
                "ClinicLocation": None,
                "FileId": None,
                "Created": datetime.now(),
                "CreatedBy": "Migration", 
                "Modified": None,  
                "ModifiedBy": None,
                "IsDeleted": False, 
            }
            
            # Logging sukses transformasi
            log_transform.info(f"[Transformed] Data assessmentHistory baris ke-{i + 1} berhasil ditransformasi.")
            data.append(transformed_data)

        except Exception as transform_error:
            # Logging error per baris
            log_transform.error(f"Error saat transformasi assessmentHistory data baris ke-{i + 1}: {transform_error}")

    return data

def crewing_Vessel():
    df = ExtractRepository.get_ship()
    data = []
    for i, row in df.iterrows():
        try:
            transformed_data = {
                "DivisionId": 1,
                "ShipName": row["Name"],
                "Id": str(uuid.uuid4()),
                "Created": datetime.now(),
                "CreatedBy": "Migration",
                "Modified": None,
                "ModifiedBy": None,
                "IsDeleted": False
            }
            log_transform.info(f"[Transformed] Data ship baris ke-{i+1}")
            data.append(transformed_data)
        except Exception as transform_error:
            log_transform.error(f"Error saat transformasi ship data baris ke-{i+1}: {transform_error}")
    return data


def crewing_employee():
    df = ExtractRepository.get_RegisterCrew()
    df = df[df["NIP"].notna() & (df["NIP"] != "")]
    data_cE = LogRepository.get_crewing_registerCrew()
    data_cE = pd.DataFrame([dict(item) for item in data_cE])
    data_cJPM = LogRepository.get_crewing_jobPositionMarine()
    data_cJPM = pd.DataFrame([dict(item) for item in data_cJPM])
    data_regDoc = ExtractRepository.get_registerDoc()
    data = []
    for i, row in df.iterrows():
        try:
            matching_doc = data_regDoc[
                (data_regDoc["CrewId"] == row["Id"]) &
                (data_regDoc["DocId"] != "169736ED-67BC-4519-9862-E4F969516F01") &
                (data_regDoc["Nomor"].str.startswith("62"))
            ]
            attachment_file_id = data_cE["RegPhotoId"].loc[data_cE["Id"] == row["Id"]].iloc[0] if not data_cE.loc[data_cE["Id"] == row["Id"]].empty else None
            transformed_data = {
                "Id": row["Id"],
                "FullName": " ".join(filter(None, [row.get('FirstName'), row.get('LastName')])).strip() 
                if row.get('FirstName') or row.get('LastName') else "Unknown",
                "Address": row["Address"] if row["Address"] else None,
                "Email": row["Email"] if row["Email"] else None,
                "Gender": "Male" if row["Gender"] == "M" else "Female" if row["Gender"] == "F" else None,
                "PlaceOfBirth": row["BirthLoc"] if row["BirthLoc"] else None,
                "DateOfBirth": convert_to_iso(row["BirthDate"]) if row["BirthDate"] else None,
                "ReligionId": religion_id_to_uuid_mapping.get(row["ReligionId"], "6CB2019D-2B83-40C9-A8BC-5C25102309B5"),
                "Nationality": "Indonesia",
                "PhotoId": attachment_file_id,
                "PassportPhotoId": None,
                "KtpPhotoId": attachment_file_id,
                "IdCardNo": row["IdentityCardNo"] if row["IdentityCardNo"] else None,
                "IdPassport": row["PasporBook"] if row["PasporBook"] else None,
                "FamilyCardNo": row["KKNumber"] if row["KKNumber"] else None,
                "NPWP": row["NPWP"].replace('.', '') if row["NPWP"] else None,
                "BPJSKesNo": row["BPJSKesNumber"] if row["BPJSKesNumber"] else None,
                "BPJSTKNo": row["BPJSKetNumber"] if row["BPJSKetNumber"] else None,
                "PhoneNo": str(row["CellPhone"]).replace('.0', '') if row["CellPhone"] else None,
                "RelativesName": None,
                "RelativesEmail": None,
                "RelativesPhone": None,
                "RelativesAddress": None,
                "BankAccount1": row["BankAccountNo"] if row["BankAccountNo"] else None,
                "BankId1": bank_id_to_uuid_mapping.get(row["BankId"], "4ED6D9F7-9B86-401C-BD15-64315043FD0E"),
                "BankAccountName1": row["BankAccountName"] if row["BankAccountName"] else None,
                "BankAccount2": row["BankAccountNo2"] if row["BankAccountNo2"] else None,
                "BankId2": bank_id_to_uuid_mapping.get(row["BankId2"], "4ED6D9F7-9B86-401C-BD15-64315043FD0E"),
                "BankAccountName2": row["BankAccountName2"] if row["BankAccountName2"] else None,
                "DivisionId": 1,
                "EmployeeIdNumber": row["NIP"],
                "OnBoardStatus": True if row["RegShipStatusId"]==0 else False,
                "ShipId": None,
                "MaritalStatusId": marital_status_id_to_uuid_mapping.get(row["MaritalStatusId"], "6EEBD3CE-9FAD-4566-ABD9-197416643170"),
                "SeaFarerId": matching_doc["Nomor"].iloc[0][:10] if not matching_doc.empty else None,
                "RegisterCrewId": row["Id"],
                "IsResigned": False,
                "EmploymentTypeId": '5E35841F-DEA8-4E72-B337-D3377E06A3BB',
                "JobPositionMarineId": int(data_cJPM.loc[data_cJPM["Hierarchy"] == row["PositionId"], "JobPositionId"].iloc[0])
                if not data_cJPM.loc[data_cJPM["Hierarchy"] == row["PositionId"]].empty else None,
                "JobPositionTankerId": None,
                "LastRejectedDate": None,
                "WilingToAcceptLowerRank": None,
                "AvailableForm": None,
                "AvailableUntil": None,
                "Weight": None,
                "Height": None,
                "BMI": None,
                "SafetyShoesSize": None,
                "TrousersSize": None,
                "CoverallSize": None,
                "ShirtSize": None,
                "Created": datetime.now(),
                "CreatedBy": "Migration",
                "IsDeleted": False,
                "Modified": None,
                "ModifiedBy": None,
                "SeamanBookNo": row["SeaManBook"] if row["SeaManBook"] else None
            }
            log_transform.info(f"[Transformed] RegisterCrew Data baris ke-{i+1}")
            data.append(transformed_data)
        except Exception as transform_error:
            log_transform.error(f"Error saat transformasi RegisterCrew data baris ke-{i+1}: {transform_error}")
    return data

COCuuid_to_docName_mapping = {
    "DD9AB9B9-E4F7-4174-80A2-3CC04D961A42": "ENDORSEMENT",
    "A80303C3-56E5-4463-96C6-F46B62D1F849": "Ijazah (tipe ijazah tidak diketahui)",
    "A80303C3-56E5-4463-96C6-F46B62D1F849CCD231DF-51D6-4F57-B9A4-E7AC91DAE096": "RASD",
    "A80303C3-56E5-4463-96C6-F46B62D1F84914E1BD41-BF82-4BEF-9102-7558FF91EFF0": "DOC I",
    "A80303C3-56E5-4463-96C6-F46B62D1F8495200306F-B1AC-4DEB-92BE-D2EE3273C34B": "DOC II",
    "A80303C3-56E5-4463-96C6-F46B62D1F849E479D469-C902-4944-815F-78F443B8AD2F": "DOC III",
    "A80303C3-56E5-4463-96C6-F46B62D1F849BC36EF55-61B7-46D2-9149-2EBE416907DE": "DOC IV",
    "A80303C3-56E5-4463-96C6-F46B62D1F849DEC6B749-F9DA-47CC-8E3C-37EFCC01E0B4": "DOC V",
    "A80303C3-56E5-4463-96C6-F46B62D1F84946926539-9F5D-4040-ACF4-025FC066C2F3": "RASE",
    "A80303C3-56E5-4463-96C6-F46B62D1F849B1D8F01F-AD1C-4451-BCF9-31641B5D8621": "EOC I",
    "A80303C3-56E5-4463-96C6-F46B62D1F8491241A596-2A01-4DFB-B3E2-E12434752944": "EOC II",
    "A80303C3-56E5-4463-96C6-F46B62D1F8490BD4BAAB-C009-44F4-8267-E8DCE4F4D381": "EOC III",
    "A80303C3-56E5-4463-96C6-F46B62D1F849CE4C72A8-0391-4E5A-8F44-135C3B6A5F69": "EOC IV",
    "A80303C3-56E5-4463-96C6-F46B62D1F8493BAD689A-3B60-46FA-BE2E-3D09AF6EA892": "EOC V",
    "A80303C3-56E5-4463-96C6-F46B62D1F849A912D717-24F2-4B69-AB98-BAA2778E8022": "RASD",
    "A80303C3-56E5-4463-96C6-F46B62D1F849C545718B-8001-43F2-BF41-6DDFD246BB0C": "RASE"
}

COCDocId_to_specificCOCId_mapping = {
    "DD9AB9B9-E4F7-4174-80A2-3CC04D961A42": 17,
    "A80303C3-56E5-4463-96C6-F46B62D1F849CCD231DF-51D6-4F57-B9A4-E7AC91DAE096": 14,
    "A80303C3-56E5-4463-96C6-F46B62D1F84914E1BD41-BF82-4BEF-9102-7558FF91EFF0": 1,
    "A80303C3-56E5-4463-96C6-F46B62D1F8495200306F-B1AC-4DEB-92BE-D2EE3273C34B": 2,
    "A80303C3-56E5-4463-96C6-F46B62D1F849E479D469-C902-4944-815F-78F443B8AD2F": 3,
    "A80303C3-56E5-4463-96C6-F46B62D1F849BC36EF55-61B7-46D2-9149-2EBE416907DE": 5,
    "A80303C3-56E5-4463-96C6-F46B62D1F849DEC6B749-F9DA-47CC-8E3C-37EFCC01E0B4": 6,
    "A80303C3-56E5-4463-96C6-F46B62D1F84946926539-9F5D-4040-ACF4-025FC066C2F3": 15,
    "A80303C3-56E5-4463-96C6-F46B62D1F849B1D8F01F-AD1C-4451-BCF9-31641B5D8621": 7,
    "A80303C3-56E5-4463-96C6-F46B62D1F8491241A596-2A01-4DFB-B3E2-E12434752944": 8,
    "A80303C3-56E5-4463-96C6-F46B62D1F8490BD4BAAB-C009-44F4-8267-E8DCE4F4D381": 9,
    "A80303C3-56E5-4463-96C6-F46B62D1F849CE4C72A8-0391-4E5A-8F44-135C3B6A5F69": 10,
    "A80303C3-56E5-4463-96C6-F46B62D1F8493BAD689A-3B60-46FA-BE2E-3D09AF6EA892": 11,
    "A80303C3-56E5-4463-96C6-F46B62D1F849A912D717-24F2-4B69-AB98-BAA2778E8022": 14,
    "A80303C3-56E5-4463-96C6-F46B62D1F849C545718B-8001-43F2-BF41-6DDFD246BB0C": 15
}

def crewing_employeeCOCDoc():
    df = ExtractRepository.get_registerDoc()
    df = df[df["DocId"].isin(['DD9AB9B9-E4F7-4174-80A2-3CC04D961A42', 'A80303C3-56E5-4463-96C6-F46B62D1F849'])]
    df_cE = LogRepository.get_crewing_employee()
    df_cE = pd.DataFrame([dict(item) for item in df_cE])
    df['CrewId'] = df['CrewId'].astype(str).str.lower()
    df = df[(df['CrewId']).isin(df_cE['Id'])]
    # print(df)
    data = []
    for i, row in df.iterrows():
        try:
            COCName = COCuuid_to_docName_mapping.get(f"{row['DocId']}{row['IjasahId'] or ''}", None)
            if not COCName:
                continue
            transformed_data = {
                "Id": str(uuid.uuid4()),
                "EmployeeId": row["CrewId"], 
                "COCName": COCName,
                "COCNumber": row["Nomor"] if row["Nomor"] else None,
                "COCId": COCDocId_to_specificCOCId_mapping.get(f"{row['DocId']}{row['IjasahId'] or ''}", None),
                "COCPublishedDate": convert_to_iso(row["Issued"]) if row["Issued"] else None,  
                "COCExpiryDate": convert_to_iso(row["Expired"]) if row["Expired"] else None,  
                "COCDescription": row["Description"] if row["Description"] else None, 
                "COCAttachment": None, 
                "Created": datetime.now(),  
                "CreatedBy": "Migration",  
                "Modified": None,  
                "ModifiedBy": None,  
                "IsDeleted": False  
            }
            # print(f"COCId = {transformed_data["COCId"]}")
            log_transform.info(f"[Transformed] Data employeeCOCDoc baris ke-{i+1}")
            data.append(transformed_data)
        except Exception as transform_error:
            log_transform.error(f"Error saat transformasi employeeCOCDoc data baris ke-{i+1}: {transform_error}")

    return data

COPuuid_to_docName_mapping = {
    "B80D3BD7-34F5-4BAA-84AF-0C1FF272A02B": "MC",
    "6353E8E2-6063-49E3-AED4-0D8C5B786ABA": "BOCT",
    "EA866E65-0B62-4622-91C5-0E13E4CDC660": "RADAR",
    "F9B89D2B-7753-4917-85B7-0F1C45BB5CC1": "KONDITE",
    "72C621FC-4ED9-4CCF-BE1F-0FA161C5B317": "ORU",
    "E40434B3-A5AD-4A0B-9F6D-12E37C1BC752": "Marlin Test",
    "C5D2F351-7792-4266-A3A0-136AF4DB0C52": "BST",
    "E7572546-BD95-4E95-85B9-1F44A202F93C": "ARPA",
    "47CA7D1A-201F-42EA-B75C-1FB95E1CACF4": "CV",
    "5EF1B87F-8B21-44D8-8140-23787CF72136": "AOT",
    "8FB09090-84B4-4681-934A-2951BDF4E956": "MCU",
    "16D9F5F9-B205-4DAA-A005-30335742D366": "ACT",
    "5B72E1B7-0AED-4DD0-80F2-34EEC5200075": "Leadership & Teamwork Training",
    "5E2E3754-0B1A-434B-9260-3765DCC77932": "SSO",
    "8FBBF102-FE02-4300-B551-3AAE52D72417": "RANGGER",
    "9152179F-120D-4153-B4D7-3C33B28AEC25": "AFF",
    "95020C92-C922-44FF-82EF-47CC21207058": "MEFA",
    "AF74F4BC-0DE7-446F-8597-47F9AE37C1E3": "SCRB",
    "E895635D-DE1F-450B-8802-565F993DE9E8": "FRC",
    "7ED3D551-2332-40F2-9BE5-6C8F846D1B62": "CTSTP",
    "07FCD3DC-3FDB-44E7-96A1-778C95273EF8": "ERM",
    "A4BEBC3F-2EDE-47D2-B3D8-98CEC75F7BFE": "GMDSS",
    "89384A94-B3D8-4FAE-8EE6-A6FF7095DF69": "SDSD",
    "481F87FA-3231-4338-BC9C-B2314ED799D1": "ECDIS",
    "3D175A25-51DD-4017-97C3-BCD09464547A": "BRM",
    "17DDBD73-5DC1-44E7-8206-BEDD6E6EA9DF": "Approval BOC Pertamina",
    "84E7E966-16A9-4124-A58C-D8F60FD2AD6D": "SBSOT",
    "E0B19778-03DD-4ADB-9AFC-E94F09F87085": "ALGT",
    "BEF454D7-C17D-4C1C-AC60-F00AA0EAB55B": "BLGT",
}

COPDocId_to_specificCOPId_mapping = {
    "16D9F5F9-B205-4DAA-A005-30335742D366": 24,
    "9152179F-120D-4153-B4D7-3C33B28AEC25": 2,
    "E0B19778-03DD-4ADB-9AFC-E94F09F87085": 28,
    "5EF1B87F-8B21-44D8-8140-23787CF72136": 23,
    "E7572546-BD95-4E95-85B9-1F44A202F93C": 7,
    "BEF454D7-C17D-4C1C-AC60-F00AA0EAB55B": 27,
    "6353E8E2-6063-49E3-AED4-0D8C5B786ABA": 22,
    "3D175A25-51DD-4017-97C3-BCD09464547A": 17,
    "C5D2F351-7792-4266-A3A0-136AF4DB0C52": 1,
    "7ED3D551-2332-40F2-9BE5-6C8F846D1B62": 26,
    "481F87FA-3231-4338-BC9C-B2314ED799D1": 10,
    "07FCD3DC-3FDB-44E7-96A1-778C95273EF8": 18,
    "E895635D-DE1F-450B-8802-565F993DE9E8": 11,
    "A4BEBC3F-2EDE-47D2-B3D8-98CEC75F7BFE": 8,
    "5B72E1B7-0AED-4DD0-80F2-34EEC5200075": 30,
    "B80D3BD7-34F5-4BAA-84AF-0C1FF272A02B": 5,
    "95020C92-C922-44FF-82EF-47CC21207058": 4,
    "72C621FC-4ED9-4CCF-BE1F-0FA161C5B317": 9,
    "EA866E65-0B62-4622-91C5-0E13E4CDC660": 6,
    "8FBBF102-FE02-4300-B551-3AAE52D72417": 13,
    "84E7E966-16A9-4124-A58C-D8F60FD2AD6D": 25,
    "AF74F4BC-0DE7-446F-8597-47F9AE37C1E3": 3,
    "89384A94-B3D8-4FAE-8EE6-A6FF7095DF69": 19,
    "5E2E3754-0B1A-434B-9260-3765DCC77932": 21
}

def crewing_employeeCOPDoc():
    df = ExtractRepository.get_registerDoc()
    df = df[
        (df["DocId"] != "17DDBD73-5DC1-44E7-8206-BEDD6E6EA9DF") &
        (df["DocId"] != "47CA7D1A-201F-42EA-B75C-1FB95E1CACF4") &
        (df["DocId"] != "E40434B3-A5AD-4A0B-9F6D-12E37C1BC752") &
        (df["DocId"] != "A80303C3-56E5-4463-96C6-F46B62D1F849") &
        (df["DocId"] != "F9B89D2B-7753-4917-85B7-0F1C45BB5CC1") &
        (df["DocId"] != "8FB09090-84B4-4681-934A-2951BDF4E956") &
        (df["DocId"] != "B2BD57D7-925B-4221-897C-B84E4E4DE69D") &
        (df["DocId"] != "169736ED-67BC-4519-9862-E4F969516F01") &
        (df["DocId"] != "DD9AB9B9-E4F7-4174-80A2-3CC04D961A42")]
    df_cE = LogRepository.get_crewing_employee()
    df_cE = pd.DataFrame([dict(item) for item in df_cE])
    data = []
    for i, row in df.iterrows():
        try:
            COPName = COPuuid_to_docName_mapping.get(row["DocId"], None)
            if not COPName:
                continue
            transformed_data = {
                "Id": str(uuid.uuid4()),
                "EmployeeId": row["CrewId"],
                "COPName": COPName,
                "COPNumber": row["Nomor"] if row["Nomor"] else None,
                "COPId": COPDocId_to_specificCOPId_mapping.get(row["DocId"], None),
                "COPPublishedDate": convert_to_iso(row["Issued"]) if row["Issued"] else None,  
                "COPExpiryDate": convert_to_iso(row["Expired"]) if row["Expired"] else None,  
                "COPDescription": row["Description"] if row["Description"] else None, 
                "COPAttachment": None, 
                "Created": datetime.now(),  
                "CreatedBy": "Migration",  
                "Modified": None,  
                "ModifiedBy": None,  
                "IsDeleted": False  
            }
            log_transform.info(f"[Transformed] Data employeeCOPDoc baris ke-{i+1}")
            data.append(transformed_data)
        except Exception as transform_error:
            log_transform.error(f"Error saat transformasi employeeCOPDoc data baris ke-{i+1}: {transform_error}")

    return data

def crewing_employeeEducation():
    log_transform = logging.getLogger("Log Transform Education")

    df = ExtractRepository.get_Education()
    city = ExtractRepository.get_City()

    city_lookup = {row["Id"]: row["Name"] for _, row in city.iterrows()}

    # Buat list data
    data = []
    for i, row in df.iterrows():
        try:
            transformed_data = {
                "Id": row["Id"],  # ID unik
                "EmployeeId": row["CrewID"] if row["CrewID"] else None,  # ID karyawan
                "EduSchool": row["SchoolName"] if row["SchoolName"] else None,  # Nama sekolah
                "EduDegree": row["Degree"] if row["Degree"] else None,  # Gelar pendidikan
                "EduMajor": None if pd.isnull(row["Prodi"]) else row["Prodi"],  # Jurusan
                "EduGPA": row["Score"] if row["Score"] else None,  # IPK
                "EduYearAdmission": convert_to_int(row["EntryYear"]),  # Tahun masuk
                "EduYearGraduate": convert_to_int(row["LeaveYear"]),  # Tahun lulus
                "EduCity": city_lookup.get(row["City"], "Unknown"),  # Lookup nama kota atau default
                "Created": datetime.now(),  # Timestamp saat data dibuat
                "CreatedBy": "Migration",  # Informasi migrasi
                "IsDeleted": isDeleted_mapping.get(row["IsDeleted"], False),  # Status penghapusan
                "Modified": datetime.now(),  # Timestamp modifikasi
                "ModifiedBy": None  # Belum ada perubahan manual
            }
            log_transform.info(f"[Transformed] Data Crewing_EmployeeEducation baris ke-{i+1}")
            data.append(transformed_data)
        except Exception as transform_error:
            log_transform.error(f"Error saat transformasi Crewing_EmployeeEducation data baris ke-{i+1}: {transform_error}")
    return data

def crewing_employeeFamily():
    df = ExtractRepository.get_RegisterCrew()
    df = df[df["NIP"].notna() & (df["NIP"] != "")]
    data = []
    for i, row in df.iterrows():
        try:
            transformed_data = {
                "Id": str(uuid.uuid4()),
                "EmployeeId": row["Id"],
                "Relationship": None,
                "Fullname": None if pd.isna(row["HomePhone"]) else row["HomePhone"],
                "Gender": None,
                "DateOfBirth": None,
                "MobileNo": None,
                "Email": None,
                "KtpNumber": None,
                "FamilyRegisterNumber": None,
                "Address": None,
                "Created": datetime.now(),
                "CreatedBy": "Migration",
                "Modified": None,
                "ModifiedBy": None,
                "IsDeleted": False
            }
            log_transform.info(f"[Transformed] Data registerFamily baris ke-{i+1}")
            data.append(transformed_data)
        except Exception as transform_error:
            log_transform.error(f"Error saat transformasi registerFamily data baris ke-{i+1}: {transform_error}")
    return data

def crewing_employeeMedicalCertificate():
        # Fetching data from repositories
        df = ExtractRepository.get_RegisterCrew()
        data_cE = LogRepository.get_crewing_registerCrew()
        data_cE = pd.DataFrame([dict(item) for item in data_cE])  # Convert to DataFrame

        data = []
        for i, row in df.iterrows():
            try:
                # Filter to find the corresponding RegPhotoId
                attachment_file_id = data_cE.loc[data_cE["Id"] == row["Id"], "RegPhotoId"].iloc[0] if not data_cE.loc[data_cE["Id"] == row["Id"]].empty else None
                
                # Prepare transformed data
                transformed_data = {
                    "Id": str(uuid.uuid4()),
                    "EmployeeId": row["Id"],
                    "MedicalCertificateName": "MCU",
                    "Number": None,
                    "PlaceOfIssue": None,
                    "DateOfIssue": None,
                    "ExpirationDate": None,
                    "AttachmentFileId": attachment_file_id,
                    "Created": datetime.now(),
                    "CreatedBy": "Migration",
                    "Modified": None,
                    "ModifiedBy": None,
                    "IsDeleted": False
                }
                
                log_transform.info(f"[Transformed] Data registerMedicalCertificate row {i + 1} transformed successfully.")
                data.append(transformed_data)

            except Exception as transform_error:
                log_transform.error(f"Error transforming row {i + 1}: {transform_error}")

        return data

def crewing_employeeSeaService():
    # Mengambil data dari repository
    df = ExtractRepository.get_registerExperience()
    df2 = ExtractRepository.get_mappingShipToVessel()
    data = []
    
    for i, row in df.iterrows():
        try:
            # Data transformasi sesuai dengan kolom di tabel Crewing_RegisterSeaService
            transformed_data = {
                "Id": str(uuid.uuid4()),
                "EmployeeId": row["CrewId"],
                "SeaServiceCompanyName": None if pd.isna(row["Company"]) else row["Company"],
                "SeaServicePosition": None if pd.isna(row["Position"]) else row["Position"], 
                "SeaServiceShipName": None if pd.isna(row["Ship"]) else row["Ship"],
                "SeaServiceShipType": None if pd.isna(row["ShipType"]) else row["ShipType"],
                "SeaServiceGRT": None if pd.isna(row["Grt"]) else row["Grt"],
                "SeaServiceOnBoard": convert_to_iso(row["WorkStart"]) if row["WorkStart"] else None,
                "SeaServiceOffBoard": convert_to_iso(row["WorkEnd"]) if row["WorkEnd"] else None,
                "SeaServiceNote": None if pd.isna(row["Description"]) else row["Description"],
                "SeaServiceAttachment": None,
                "VesselId": df2.loc[df2["ShipName"] == row["Ship"], "VesselId"].iloc[0] if not df2.loc[df2["ShipName"] == row["Ship"]].empty else None,
                "Created": datetime.now(),
                "CreatedBy": "Migration",
                "Modified": None,
                "ModifiedBy": None,
                "IsDeleted": False,
                "SeaServiceDWT": None,
                "SeaServiceHP": None,
            }
            
            # Logging berhasil transformasi
            log_transform.info(f"[Transformed] Data registerSeaService baris ke-{i+1} berhasil ditransformasi.")
            data.append(transformed_data)

        except Exception as transform_error:
            # Logging error per baris
            log_transform.error(f"Error saat transformasi registerSeaService data baris ke-{i+1}: {transform_error}")

    return data

trainingId_to_trainingCategory_mapping = {
    1: "Hard Skill",
    2: "Soft Skill",
    3: "Other Skill"
}


def crewing_employeeTraining():
    df = ExtractRepository.get_RegisterTraining()
    data = []
    
    for i, row in df.iterrows():
        try:
            # Data transformasi sesuai dengan kolom di tabel Crewing_RegisterTraining
            transformed_data = {
                "Id": str(uuid.uuid4()),
                "EmployeeId": row["CrewId"].lower(),
                "TrainingInstitution": row["Institution"] if row["Institution"] else None,
                "TrainingPosition": row["TrainingName"] if row["TrainingName"] else None,
                "TrainingCategory": trainingId_to_trainingCategory_mapping.get(row["TrainingCategoryId"], None),
                "TrainingInstructor": None if pd.isna(row["InstructorName"]) else row["InstructorName"],  
                "TrainingFeedback": None if pd.isna(row["TrainingFeedBack"]) else row["TrainingFeedBack"],
                "TrainingEval": None if pd.isna(row["Evaluation"]) else row["Evaluation"],
                "TrainingStartDate": convert_to_iso(row["StartDate"]) if row["StartDate"] else None,  # Tanggal mulai training
                "TrainingEndDate": convert_to_iso(row["EndDate"]) if row["EndDate"] else None,  # Tanggal selesai training
                "Created": datetime.now(),
                "CreatedBy": "Migration",
                "Modified": None,
                "ModifiedBy": None,
                "IsDeleted": False
            }
            
            # Logging berhasil transformasi
            log_transform.info(f"[Transformed] Data Training baris ke-{i+1} berhasil ditransformasi.")
            data.append(transformed_data)

        except Exception as transform_error:
            # Logging error per baris
            log_transform.error(f"Error saat transformasi Training data baris ke-{i+1}: {transform_error}")

    return data

traverDocuuid_to_docName_mapping = {
    "B2BD57D7-925B-4221-897C-B84E4E4DE69D": "PASPORT",
    "169736ED-67BC-4519-9862-E4F969516F01": "SEA MAN BOOK",
}
def crewing_employeeTravelDoc():
    df = LogRepository.get_crewing_employeeCOCDoc()
    df = pd.DataFrame([dict(item) for item in df])
    df = df[df["COCName"].isin(['PASPORT', 'SEA MAN BOOK'])]
    data = []
    for i, row in df.iterrows():
        try:
            transformed_data = {
                "Id": str(uuid.uuid4()),
                "EmployeeId": row["EmployeeId"],
                "TravelDocName": None if pd.isna(row["COCName"]) else row["COCName"],
                "TravelDocNo": None if pd.isna(row["COCNumber"]) else row["COCNumber"],
                "DocDateIssued": None if pd.isna(row["COCPublishedDate"]) else row["COCPublishedDate"], 
                "DocDateExpired": None if pd.isna(row["COCExpiryDate"]) else row["COCExpiryDate"],
                "TravelInformation": None if pd.isna(row["COCDescription"]) else row["COCDescription"],
                "Created": datetime.now(),
                "CreatedBy": "Migration",
                "Modified": None, 
                "ModifiedBy": None,
                "IsDeleted": False
            }
            log_transform.info(f"[Transformed] Data TravelDoc baris ke-{i+1}")
            data.append(transformed_data)
        except Exception as transform_error:
            log_transform.error(f"Error saat transformasi TravelDoc data baris ke-{i+1}: {transform_error}")
    return data

def update_employeeShipId():
    # Ambil data dari repository
    df = ExtractRepository.get_registerCrew_mapShipCategory()
    df_ship = LogRepository.get_crewing_ship()
    df_ship = pd.DataFrame([dict(item) for item in df_ship])
    data = []
    for i, row in df.iterrows():
        try:
            transformed_data = {
                "Id": row["Id"],
                "ShipId": int(df_ship.loc[df_ship["ShipName"] == row["CATEGORY"], "ShipId"].iloc[0] 
                if not df_ship.loc[df_ship["ShipName"] == row["CATEGORY"], "ShipId"].empty else None)
            }
            log_transform.info(f"[Transformed] Data RegisterCrew baris ke-{i+1}")
            data.append(transformed_data)
        except Exception as transform_error:
            log_transform.error(f"Error saat transformasi RegisterCrew data baris ke-{i+1}: {transform_error}")
    return data

blacklistuuid_to_int_mapping = {
    "B64941C1-47F0-4084-8890-85F4694387F0": 1,
    "97F89829-AD8D-4C7C-9B44-1A6F9C6733CC": 2,
    "8E167EFA-FBC3-49B6-BF87-7DC30FEE6E78": 4,
}

mapping_JobPositionMarineId = {
    1: None,  # Director (Tidak ada padanan yang sesuai)
    2: None,  # Vp/Head of Auditor/Director AP (Tidak ada padanan yang sesuai)
    3: None,  # Manager/Senior Auditor/Ka. Cabang (Tidak ada padanan yang sesuai)
    4: None,  # Ast. Manager/Auditor (Tidak ada padanan yang sesuai)
    5: None,  # Assistant/Junior Auditor (Tidak ada padanan yang sesuai)
    6: None,  # Kontrak (Tidak ada padanan yang sesuai)
    7: None,  # Outsourcing (Tidak ada padanan yang sesuai)
    8: 1,  # MASTER → NAKHODA
    9: 2,  # CHIEF OFFICER → MUALIM I
    10: 3,  # 2ND OFFICER → MUALIM II
    11: 4,  # 3RD OFFICER → MUALIM III
    12: 5,  # CHIEF ENGINEER → KKM
    13: 7,  # 2ND ENGINEER → MASINIS II
    14: 8,  # 3RD ENGINEER → MASINIS III
    15: 9,  # 4TH ENGINEER → MASINIS IV
    16: 12,  # A/B → JURU MUDI
    17: 13,  # OILER → JURU MINYAK
    18: 19,  # PUMP MAN → JURU POMPA
    19: 11,  # BOSUN → BOSUN
    20: 17,  # JURAGAN → SKIPPER
    21: 18,  # MANDOR → MANDOR MESIN (ENGINE FOREMAN)
    22: 16,  # MESS MAN (Tidak ada padanan yang sesuai)
    23: 15,  # FITTER → FITTER
    24: 14,  # KOKI → JURU MASAK (COOK)
    25: 10,  # ELECTRICIAN → ELECTRICIAN
    26: 32,  # ASESOR TCMS/CMMS (Tidak ada padanan yang sesuai)
    27: 33,  # TS SHIP MANAGEMENT TYM (Tidak ada padanan yang sesuai)
    28: 34   # ASESOR HSSE (Tidak ada padanan yang sesuai)
}

def update_JobPositionMarineId():
    # Ambil data dari repository
    df = ExtractRepository.get_RegisterCrew()
    data = []
    for i, row in df.iterrows():
        try:
            transformed_data = {
                "Id": row["Id"],
                "JobPositionMarineId": mapping_JobPositionMarineId.get(row["PositionId"], None)
            }
            log_transform.info(f"[Transformed] Data RegisterCrew baris ke-{i+1}")
            data.append(transformed_data)
        except Exception as transform_error:
            log_transform.error(f"Error saat transformasi RegisterCrew data baris ke-{i+1}: {transform_error}")
    return data

def update_JobPositionMarineId_BS():
    # Ambil data dari repository
    df = LogRepository.get_crewing_employee()
    df = pd.DataFrame([dict(item) for item in df])
    data = []
    for i, row in df.iterrows():
        try:
            transformed_data = {
                "Id": row["Id"],
                "JobPositionMarineId": None if pd.isna(row["JobPositionMarineId"]) else int(row["JobPositionMarineId"])
            }
            log_transform.info(f"[Transformed] Data RegisterCrew baris ke-{i+1}")
            data.append(transformed_data)
        except Exception as transform_error:
            log_transform.error(f"Error saat transformasi RegisterCrew data baris ke-{i+1}: {transform_error}")
    return data

def crewing_employeeBlacklist():
    df = ExtractRepository.get_RegisterCrew()
    df = df[
    (df["StatusRegister"] == "B64941C1-47F0-4084-8890-85F4694387F0") |
    (df["StatusRegister"] == "97F89829-AD8D-4C7C-9B44-1A6F9C6733CC") |
    (df["StatusRegister"] == "8E167EFA-FBC3-49B6-BF87-7DC30FEE6E78")]
    data = []
    for i, row in df.iterrows():
        try:
            transformed_data = {
                "Id": str(uuid.uuid4()),
                "EmployeeId": row["Id"],
                "EmployeeBlacklistLevelId": blacklistuuid_to_int_mapping.get(row["StatusRegister"]),
                "StartDate": None,
                "EndDate": None,
                "Created": datetime.now(),
                "CreatedBy": "Migration",
                "Modified": None,
                "ModifiedBy": None,
                "IsDeleted": False
            }
            log_transform.info(f"[Transformed] Data Blacklist baris ke-{i+1}")
            data.append(transformed_data)
        except Exception as transform_error:
            log_transform.error(f"Error saat transformasi Blacklist data baris ke-{i+1}: {transform_error}")
    return data

activeStatusuuid_to_bool_mapping = {
    "027C7105-0F55-44F1-A286-492BB8DDB8D8": True,
    "6A422334-62D7-46CA-A07B-167E15690627": False,
}

def crewing_employeeBoardSchedule():
    # Mengambil data dari repository
    df = ExtractRepository.get_RegisterCrew()
    df = df[df["NIP"].notna() & (df["NIP"] != "")]
    df = df[
        (df["StatusRegister"] == "027C7105-0F55-44F1-A286-492BB8DDB8D8") |
        (df["StatusRegister"] == "6A422334-62D7-46CA-A07B-167E15690627") ]
    df2 = ExtractRepository.get_mappingShipToVessel()
    data = []
    for i, row in df.iterrows():
        try:
            # Membuat transformed_data
            transformed_data = {
                "Id": str(uuid.uuid4()),
                "EmployeeId": row["Id"],
                "OnBoardDate": convert_to_iso(row["OnBoard"]) if row["OnBoard"] else None,
                "OffBoardDate": convert_to_iso(row["OffBoard"]) if row["OffBoard"] else None,
                "IsActive": activeStatusuuid_to_bool_mapping.get(row["StatusRegister"]),
                "VesselId": df2.loc[df2["ShipId"] == row["ShipId"], "VesselId"].iloc[0] if not df2.loc[df2["ShipId"] == row["ShipId"]].empty else None,
                "JobPositionMarineId": mapping_JobPositionMarineId.get(row["PositionId"], None),
                "Created": datetime.now(),
                "CreatedBy": "Migration",
                "Modified": None,
                "ModifiedBy": None,
                "IsDeleted": False,
                "IsOnBoard": True if row["RegShipStatusId"] == 0 else False,
                "OriginalOffBoardDate": None,
            }

            if transformed_data["VesselId"] is not None and pd.notna(transformed_data["VesselId"]):
                log_transform.info(f"[Transformed] Data BoardSchedule baris ke-{i+1}")
                data.append(transformed_data)
            else:
                log_transform.warning(f"Data baris ke-{i+1} di-skip karena VesselId None atau nan")

        except Exception as transform_error:
            log_transform.error(f"Error saat transformasi BoardSchedule data baris ke-{i+1}: {transform_error}")
    return data

bank_name_to_uuid_mapping = {
    "MANDIRI/MDR-ID-JKT-0039": "3B9DF32A-AEE7-432C-9D4E-24AE970D64E2",
    "BRI/BRI-ID-JKT-0045": "4ED6D9F7-9B86-401C-BD15-64315043FD0E",
    "BCA/BCA-ID-JKT-0004": "95900ADD-5CD7-41DC-9750-86D674694B77",
    "BNI/BNI-ID-JKT-0024": "412E9296-89A6-4657-99D2-27D322457E3D",
    "BSI/BSI-ID-JKT-5502": "745E032D-27C6-43CD-BCC4-0EEAEEFCC277",
    "BANK JAWA TENGAH": "65E59B9B-9F2A-4771-AF8C-680388578E3A",
    "BTN": "FF0AB086-9C9D-4096-9699-0B00BB4C39C3",
    "Maybank Jakarta": "8A0989CE-0E7C-4E6E-B701-5095E807A2EB",
    "PANIN": "PANIN"
}

jenis_kapal_to_ship_id_mapping = {
    "CHEMICAL": 5,
    "OIL TANKER": 3,
    "GAS TANKER ": 2
}

rank_to_jobPositionId_mapping = {
    "Able Seaman": 16,
    "Boatswain": 14,
    "Chief Engineer": 2,
    "Chief Officer": 3,
    "Cook": 19,
    "Electrician": 12,
    "Electro Technical Officer": 12,
    "Fitter": 13,
    "Fourth Engineer": 10,
    "Gas Engineer": 8,
    "Master": 1,
    "Messboy": 20,
    "Oiler": 18,
    "Ordinary Seaman": 17,
    "Pumpman": 15,
    "Second Engineer": 4,
    "Second Officer": 5,
    "Senior Deck Cadet": 7,
    "Senior Engine Cadet": 11,
    "Third Engineer": 9,
    "Third Officer": 6
}

def crewTanker():
    df = ExtractRepository.get_crewTanker()
    data = []
    for i, row in df.iterrows():
        try:
            transformed_data = {
                "Id": str(uuid.uuid4()),
                "FullName": None if pd.isna(row["Name"]) else row["Name"],
                "Address": None,
                "Email": None if pd.isna(row["Email"]) else row["Email"],
                "Gender": None if pd.isna(row["Gender"]) else row["Gender"],
                "PlaceOfBirth": None if pd.isna(row["City Of Birth"]) else row["City Of Birth"],
                "DateOfBirth": None if pd.isna(row["Date Of Birth"]) else row["Date Of Birth"],
                "ReligionId": None,
                "Nationality": "WNI",
                "PhotoId": None,
                "PassportPhotoId": None,
                "KtpPhotoId": None,
                "IdCardNo": None,
                "IdPassport": None if pd.isna(row["Passport.No."]) else row["Passport.No."],
                "FamilyCardNo": None if pd.isna(row["Family Register (KK).No."]) else row["Family Register (KK).No."],
                "NPWP": None,
                "BPJSKesNo": None if pd.isna(row["BPJS Kesehatan.No."]) else row["BPJS Kesehatan.No."],
                "BPJSTKNo": None if pd.isna(row["BPJS Ketenagakerjaan.No."]) else row["BPJS Ketenagakerjaan.No."],
                "PhoneNo": None,
                "RelativesName": None if pd.isna(row["Full Name"]) else row["Full Name"],
                "RelativesEmail": None if pd.isna(row["Email.1"]) else row["Email.1"],
                "RelativesPhone": str(row["Mobile No."]).replace('(+62)', '0') if row["Mobile No."] else None,
                "RelativesAddress": None if pd.isna(row["Address"]) else row["Address"],
                "BankAccount1": None if pd.isna(row["Account Number"]) else str(row["Account Number"]),
                "BankId1": bank_name_to_uuid_mapping.get(row["Bank Name"], "3B9DF32A-AEE7-432C-9D4E-24AE970D64E2"),
                "BankAccountName1": None if pd.isna(row["Account Name"]) else row["Account Name"],
                "BankAccount2": None,
                "BankId2": None,
                "BankAccountName2": None,
                "DivisionId": 2,
                "EmployeeIdNumber": None,
                "OnBoardStatus": True if row["Planned sign off"] > datetime.now() else False,
                "ShipId": jenis_kapal_to_ship_id_mapping.get(row["Jenis"], None),
                "MaritalStatusId": None,
                "SeaFarerId": None if pd.isna(row["Seafarer ID"]) else str(row["Seafarer ID"]),
                "RegisterCrewId": None,
                "IsResigned": False,
                "EmploymentTypeId": '5E35841F-DEA8-4E72-B337-D3377E06A3BB' if row["Contract Status"] == "PWT" else '04AD0F4A-C8FD-4197-A520-613EDE0B57FA',
                "JobPositionMarineId": None,
                "JobPositionTankerId": rank_to_jobPositionId_mapping.get(row["Rank"], None),
                "LastRejectedDate": None,
                "WilingToAcceptLowerRank": None,
                "AvailableForm": None,
                "AvailableUntil": None,
                "Weight": None,
                "Height": None,
                "BMI": None,
                "SafetyShoesSize": None,
                "TrousersSize": None,
                "CoverallSize": None,
                "ShirtSize": None,
                "Created": datetime.now(),
                "CreatedBy": "Migration",
                "IsDeleted": False,
                "Modified": None,
                "ModifiedBy": None,
                "SeamanBookNo": None if pd.isna(row["Seaman's Book.No."]) else row["Seaman's Book.No."],
            }
            log_transform.info(f"[Transformed] Data Tanker baris ke-{i+1}")
            data.append(transformed_data)
        except Exception as transform_error:
            log_transform.error(f"Error saat transformasi Tanker data baris ke-{i+1}: {transform_error}")
    return data

crewTanker_kapal_mapping = {
    "GAS ARAR": "8748A4CF-254F-45A3-B79C-8432C0A51F05",
    "GAS ARIMBI": "134F6E19-C0A5-4CAD-BF42-0BD77B1BB8DC",
    "GAS ATTAKA": "B07100A1-7209-449D-B91A-1E03331C323D",
    "GAS PATRA 2": "FFEF949B-A140-4467-B473-02280D886732",
    "GAS PATRA 3": "0ABF12AA-6B00-4F72-B4FB-8175CB3B5BB4",
    "GAS WALIO ": "BBCAF872-4982-4382-81A0-74A9639416E9",
    "GAS WIDURI": "453F76DD-6ED5-49F2-B41F-2FBEB663FD2A",
    "MT CENDRAWASIH": "BF85F4A5-D096-4773-8B2C-49E08C9E97B6",
    "MT ENDURO": "3C032EE3-85FF-4B25-BF3C-2D60330FEDF7",
    "MT FASTRON": "074CA89E-C50F-4E3B-AD07-03B9637074B3",
    "MT GUNUNG KEMALA": "57EF88BC-42EA-470E-AE9F-92E837D81B6F",
    "MT KAMOJANG": "DE674E03-7540-4B0A-978D-6816867D4F19",
    "MT KATOMAS": "F1804ECC-A37A-4687-8491-1E40F6A4C296",
    "MT KETALING": "1E6E50E0-DDF5-411D-A245-ED3FE9512868",
    "MT KLAWOTONG": "0A650C9A-2A5F-405C-82BA-2AA230C654B6",
    "MT KRASAK": "34C57CD9-21B1-4333-909A-13021012D9E8",
    "MT KUANG": "11CFCCE4-E4D5-4772-9A7D-096797057A3B",
    "MT MERAUKE": "BE878570-786A-4F13-8C19-A40748513C45",
    "MT MUSI": "0BF5467B-30F5-4F30-84E0-3BC49EB39A83",
    "MT PAGERUNGAN": "8534385C-1028-4204-9B14-8A56C65FAE40",
    "MT PANDAN": "1965E6F0-AA98-41CE-8906-01823A3BABC1",
    "MT PANGKALAN BRANDAN": "CD7BF292-1AA3-47A0-B6D3-04DD02219A9C",
    "MT PASAMAN": "2DBF8D38-9112-48E0-BA33-83927829AAF2",
    "MT SANGA SANGA": "8C094C0D-9AFB-4764-BC37-B10A93EE8129",
    "MT SENGETI": "059CAB8D-BE41-4A69-B258-D985A0B57E5D",
    "MT SEPINGGAN": "E4C0EBF0-1F18-41F7-8808-B32845780245",
    "MT SERANG JAYA": "011F88F7-7315-418E-9148-DD773AFE7726",
    "MT SUNGAI GERONG": "773BC877-A954-4F53-A361-943112F35948",
    "PATRA TANKER 1": "904B2031-67F3-488F-A142-4BF7F3E7AA04",
    "PATRA TANKER 2": "6E69D0D6-5251-4399-AC52-DA6EDA5E0657",
    "PATRA TANKER 3": "EFD1B97B-D00F-490E-A028-43093A904476",
    "PERTAMINA GAS 1": "44167575-0D2A-410B-A50C-882C0C72F9D9",
    "PERTAMINA GAS 2": "8752BCF2-CC71-4EF7-BE22-BF2AC493BE4E",
    "PIS MAHAKAM": "C5218FD2-8577-4675-91A6-3B0503876DC3",
    "TRANSKO ANTASENA": "F9E5A593-E34B-4C3D-B8D1-5EC27F237077",
    "TRANSKO AQUILA": "770C1A26-1E77-48A2-92E5-CE574DF24CAA",
    "TRANSKO ARAFURA": "C2E1BEC7-D11A-4E4E-85E9-64F198AF8A8D",
    "TRANSKO ARIES": "76E959ED-299C-4B0A-B4D1-AAB5DA2CFBB5",
    "TRANSKO BIMA": "5FF64CF1-0533-4A27-8C01-CF948810D291",
    "TRANSKO TAURUS": "1789ADED-BECD-4BC0-88CD-796DE3B863CF",
    "MT KAKAP": "9565D20A-D383-4A3B-992E-D28F20AD0554",
    "MT PARIGI": "4F522724-4FFF-4771-AAA5-1F8C8F60B4B7",
    "MT SEI PAKNING": "EBB69FD6-B4B3-4984-BC9B-7F423F424D9A",
    "Pertamina Gas Dahlia": "3ADA89AD-EF28-4F92-915E-45E9CD257A61"
}

def crewTankerBoardSchedule():
    df = ExtractRepository.get_crewTanker()
    df_emp = LogRepository.get_crewing_employee()
    df_emp = pd.DataFrame([dict(item) for item in df_emp])
    df2 = ExtractRepository.get_mappingShipToVessel()
    data = []
    for i, row in df.iterrows():
        try:
            # Membuat transformed_data
            transformed_data = {
                "Id": str(uuid.uuid4()),
                "EmployeeId": df_emp.loc[df_emp["FullName"] == row["Name"], "Id"].iloc[0] if not df_emp.loc[df_emp["FullName"] == row["Name"]].empty else None,
                "OnBoardDate": None if pd.isna(row["Actual sign on"]) else row["Actual sign on"],
                "OffBoardDate": None if pd.isna(row["Planned sign off"]) else row["Planned sign off"],
                "IsActive": True,
                "VesselId": crewTanker_kapal_mapping.get(row["Kapal"], None),
                "JobPositionTankerId": rank_to_jobPositionId_mapping.get(row["Rank"], None),
                "Created": datetime.now(),
                "CreatedBy": "Migration",
                "Modified": None,
                "ModifiedBy": None,
                "IsDeleted": False,
                "IsOnBoard": True,
                "OriginalOffBoardDate": None,
            }
            if transformed_data["EmployeeId"] and transformed_data["VesselId"]:
                log_transform.info(f"[Transformed] Data BoardSchedule baris ke-{i+1}")
                data.append(transformed_data)
            else:
                log_transform.warning(f"Data baris ke-{i+1} di-skip karena EmployeeId atau VesselId tidak valid")

        except Exception as transform_error:
            log_transform.error(f"Error saat transformasi BoardSchedule data baris ke-{i+1}: {transform_error} | Data: {row}")
    return data
