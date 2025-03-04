from prisma import Prisma
import pandas as pd
import uuid
from datetime import datetime

def kurang_800():
    prisma = Prisma()
    prisma.connect()

    df = pd.read_excel("./data_source/800_kurang.xlsx")
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
                "OnBoardStatus": False,
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
            print(f"[Transformed] Data Tanker baris ke-{i+1}")
            data.append(transformed_data)
        except Exception as transform_error:
            print(f"Error saat transformasi Tanker data baris ke-{i+1}: {transform_error}")
    return data


    # prisma.disconnect()

kurang_800()
