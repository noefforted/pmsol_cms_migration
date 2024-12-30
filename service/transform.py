import uuid
from datetime import datetime
from repository.extract_repository import ExtractRepository

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

async def crewing_registerCrew():
    df = await ExtractRepository.get_RegisterCrew()
    data = [
        {
            "Id": row["Id"],
            "RegCrewNo": row["RegCrewNo"],
            "RegRegisNo": row["RegRegisNo"],
            "RegFullName": row["RegFullName"],
            "RegAddress": row["RegAddress"],
            "RegEmail": row["RegEmail"],
            "RegGender": row["RegGender"],
            "RegPlaceOfBirth": row["RegPlaceOfBirth"],
            "RegDateOfBirth": row["RegDateOfBirth"],
            "RegReligionId": row["RegReligionId"],
            "RegMaritalStatusId": row["RegMaritalStatusId"],
            "RegNationality": row["RegNationality"],
            "RegPhotoId": row["RegPhotoId"],
            "RegPassportPhotoId": row["RegPassportPhotoId"],
            "RegKtpPhotoId": row["RegKtpPhotoId"],
            "RegIdCardNo": row["RegIdCardNo"],
            "RegIdPassport": row["RegIdPassport"],
            "RegFamilyCardNo": row["RegFamilyCardNo"],
            "RegNPWP": row["RegNPWP"],
            "RegBPJSKesNo": row["RegBPJSKesNo"],
            "RegBPJSTKNo": row["RegBPJSTKNo"],
            "RegPhoneNo": row["RegPhoneNo"],
            "RegRelativesName": row["RegRelativesName"],
            "RegRelativesEmail": row["RegRelativesEmail"],
            "RegRelativesPhone": row["RegRelativesPhone"],
            "RegRelativesAddress": row["RegRelativesAddress"],
            "RegBankAccount1": row["RegBankAccount1"],
            "RegBankId1": row["RegBankId1"],
            "RegBankAccountName1": row["RegBankAccountName1"],
            "RegBankAccount2": row["RegBankAccount2"],
            "RegBankId2": row["RegBankId2"],
            "RegBankAccountName2": row["RegBankAccountName2"],
            "RegDivisionId": row["RegDivisionId"],
            "CurrentStatusId": row["CurrentStatusId"],
            "JobOpeningId": row["JobOpeningId"],
            "StatusUrl": row["StatusUrl"],
            "RegApplyDate": row["RegApplyDate"],
            "RegSeaFarerId": row["RegSeaFarerId"],
            "EmployeeIdNumber": row["EmployeeIdNumber"],
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
            "IsDeleted": row["IsDeleted"],
            "Modified": datetime.now(),
            "ModifiedBy": None,
            "RegSeamanBookNo": row["RegSeamanBookNo"],
            "TemporaryPassword": row["TemporaryPassword"]
        }
        for _, row in df.iterrows()
    ]
    return data
