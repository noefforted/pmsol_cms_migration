import uuid
from datetime import datetime
from repository.extract_repository import ExtractRepository
from repository.log_repository import LogRepository
import pandas as pd
import logging

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
                "RegNationality": None,
                "RegPhotoId": None,
                "RegPassportPhotoId": None,
                "RegKtpPhotoId": None,
                "RegIdCardNo": row["IdentityCardNo"] if row["IdentityCardNo"] else None,
                "RegIdPassport": row["PasporBook"] if row["PasporBook"] else None,
                "RegFamilyCardNo": row["KKNumber"] if row["KKNumber"] else None,
                "RegNPWP": row["NPWP"] if row["NPWP"] else None,
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


def crewing_registerCrewEducation():
    log_transform = logging.getLogger("Log Transform Education")

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

def crewing_employee():
    df = ExtractRepository.get_RegisterCrew()
    data = []
    for i, row in df.iterrows():
        try:
            transformed_data = {
                "Id": row["Id"],
                "RegFullName": f"{row['FirstName']} {row['LastName']}".strip() if row['FirstName'] or row['LastName'] else "Unknown",
                "Address": row["Address"] if row["Address"] else None,
                "Email": row["Email"] if row["Email"] else None,
                "Gender": row["Gender"] if row["Gender"] else None,
                "PlaceOfBirth": row["BirthLoc"] if row["BirthLoc"] else None,
                "DateOfBirth": convert_to_iso(row["BirthDate"]) if row["BirthDate"] else None,
                "ReligionId": religion_id_to_uuid_mapping.get(row["ReligionId"], "6CB2019D-2B83-40C9-A8BC-5C25102309B5"),
                "Nationality": None,
                "PhotoId": None,
                "PassportPhotoId": None,
                "KtpPhotoId": None,
                "IdCardNo": row["IdentityCardNo"] if row["IdentityCardNo"] else None,
                "IdPassport": row["PasporBook"] if row["PasporBook"] else None,
                "FamilyCardNo": row["KKNumber"] if row["KKNumber"] else None,
                "NPWP": row["NPWP"] if row["NPWP"] else None,
                "BPJSKesNo": row["BPJSKesNumber"] if row["BPJSKesNumber"] else None,
                "BPJSTKNo": row["BPJSKetNumber"] if row["BPJSKetNumber"] else None,
                "PhoneNo": str(row["CellPhone"]).strip() if pd.notnull(row["CellPhone"]) else None,
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
                "DivisionId": row["DivisionId"],
                "EmployeeIdNumber": row["NIP"],
                "OnBoardStatus": bool(row["OnBoard"]),
                "ShipId": row["ShipId"],
                "MaritalStatusId": marital_status_id_to_uuid_mapping.get(row["MaritalStatusId"], "6EEBD3CE-9FAD-4566-ABD9-197416643170"),
                "SeaFarerId": row["SeaManBook"],
                "RegisterCrewId": row["Id"],
                "IsResigned": False,
                "EmploymentTypeId": employment_type_mapping.get(row["EmploymentTypeId"]),
                "JobPositionMarineId": row["PositionId"] if row["PositionId"] in marine_position_mapping else None,
                "JobPositionTankerId": row["PositionId"] if row["PositionId"] in tanker_position_mapping else None,
                "LastRejectedDate": None,
                "WilingToAcceptLowerRank": row["WilingToAcceptLowerRank"],
                "AvailableForm": row["AvailableForm"],
                "AvailableUntil": row["AvailableUntil"],
                "Weight": row["Weight"],
                "Height": row["Height"],
                "BMI": row["BMI"],
                "SafetyShoesSize": row["SafetyShoesSize"],
                "TrousersSize": row["TrousersSize"],
                "CoverallSize": row["CoverallSize"],
                "ShirtSize": row["ShirtSize"],
                "Created": datetime.now(),
                "CreatedBy": "Migration",
                "IsDeleted": row["IsDeleted"] == "Y",
                "Modified": datetime.now(),
                "ModifiedBy": None,
                "SeamanBookNo": row["SeaManBook"]
            }
            log_transform.info(f"[Transformed] RegisterCrew Data baris ke-{i+1}")
            data.append(transformed_data)
        except Exception as transform_error:
            log_transform.error(f"Error saat transformasi RegisterCrew data baris ke-{i+1}: {transform_error}")
    return data