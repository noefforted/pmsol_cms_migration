import os
import shutil
from prisma import Prisma
from repository.extract_repository import ExtractRepository

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

def copy_latest_file():
    source_path = "/media/ahmadaufa/J Gab/FileUpload/F7PQ3azX7krRqesRDEYA/Documents/Sertifikat"
    destination_root = "/media/ahmadaufa/J Gab/NAHKODA_Files/cop"

    db = Prisma()
    db.connect()

    try:
        df1 = ExtractRepository.get_mstDoc()
        df2 = ExtractRepository.get_registerDoc()
        df2 = df2[df2["CrewId"].notnull() & (df2["CrewId"] != "")]

        target_doc_type = "D774A7CE-9636-4AEA-9394-C553CD6C574F"
        filtered_doc_ids = df1[df1["DocType"] == target_doc_type]["DocId"].tolist()

        if not filtered_doc_ids:
            print("No matching DocIds found in df1.")
            return

        filtered_df2 = df2[df2["DocId"].isin(filtered_doc_ids)]

        if filtered_df2.empty:
            print("No matching rows found in df2.")
            return

        # Cek folder yang sudah ada sebelum iterasi
        existing_folders = set(os.listdir(destination_root))

        for _, row in filtered_df2.iterrows():
            employee_id = row["CrewId"]
            user_doc_id = row["UserDocId"]
            doc_id = row["DocId"]
            COP_name = COPuuid_to_docName_mapping.get(doc_id, None)

            search_pattern = f"{employee_id}{user_doc_id}".strip().lower()
            match_row = db.crewing_employeecopdoc.find_first(where={"EmployeeId": employee_id, "COPName": COP_name})

            if not match_row or not match_row.COPAttachment:
                print(f"No COPRow found for EmployeeId {employee_id}")
                continue

            COP_attachment = match_row.COPAttachment
            destination_path = os.path.join(destination_root, COP_attachment)

            # Skip jika folder sudah ada
            if COP_attachment in existing_folders:
                print(f"Skipping existing folder: {destination_path}")
                continue

            os.makedirs(destination_path, exist_ok=True)

            matched_files = [
                os.path.join(source_path, f)
                for f in os.listdir(source_path)
                if search_pattern in f.lower()
            ]

            if matched_files:
                latest_file = max(matched_files, key=os.path.getmtime)
                destination_file = os.path.join(destination_path, os.path.basename(latest_file))
                shutil.copy2(latest_file, destination_file)
                print(f"Copied latest file {latest_file} to {destination_path}")

        print("Latest files copied successfully.")
    except Exception as e:
        print(f"Error during file copy: {e}")
    finally:
        db.disconnect()

if __name__ == "__main__":
    copy_latest_file()
