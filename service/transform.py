import uuid
from datetime import datetime
from repository.extract_repository import ExtractRepository
from repository.log_repository import LogRepository
import pandas as pd

async def crewing_city():
    df = await ExtractRepository.get_City()
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

async def crewing_school():
    df = await ExtractRepository.get_Education()
    data = [
        {
            "Id": str(uuid.uuid4()),
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

async def crewing_registerCrew():
    df = await ExtractRepository.get_RegisterCrew()
    df = df.iloc[:150]
    data = [
        {
            "Id": row["Id"],
            "RegCrewNo": await LogRepository.get_last_regCrewNo() + i + 1,
            "RegRegisNo": row["NIP"] if row["NIP"] else None,
            "RegFullName": f"{row['FirstName']} {row['LastName']}".strip() if row['LastName'] else row['FirstName'],
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
            "RegIdCardNo": row["IdentityCardNo"],
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
            "RegDivisionId": 1,
            "CurrentStatusId": 1, #dummy
            "JobOpeningId": "2184F223-4B8D-4C1A-9542-3C3DF101492B",
            "StatusUrl": None,
            "RegApplyDate": None,
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
            "IsDeleted": False,
            "Modified": datetime.now(),
            "ModifiedBy": None,
            "RegSeamanBookNo": None,
            "TemporaryPassword": None
        }
        for i, row in df.iterrows()
    ]
    return data
