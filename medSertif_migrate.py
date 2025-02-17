import os
import shutil
from prisma import Prisma
from repository.extract_repository import ExtractRepository

def copy_latest_file():
    source_path = "/media/ahmadaufa/J Gab/FileUpload/F7PQ3azX7krRqesRDEYA/Documents/Sertifikat"
    destination_root = "/media/ahmadaufa/J Gab/NAHKODA_Files/personaldata"

    db = Prisma()
    db.connect()

    try:
        df1 = ExtractRepository.get_mstDoc()
        df2 = ExtractRepository.get_registerDoc()
        df2 = df2[df2["CrewId"].notnull() & (df2["CrewId"] != "")]

        target_doc_type = "D774A7CE-9636-4AEA-9394-C553CD6C574F"
        filtered_df1 = df1[(df1["DocType"] == target_doc_type) & (df1["DocName"] == "MCU")]

        filtered_doc_ids = filtered_df1["DocId"].tolist()
        print(f"Filtered DocIds: {filtered_doc_ids}")

        if not filtered_doc_ids:
            print("No matching DocIds found in df1.")
            return

        filtered_df2 = df2[df2["DocId"].isin(filtered_doc_ids)]

        if filtered_df2.empty:
            print("No matching rows found in df2.")
            return

        for _, row in filtered_df2.iterrows():
            employee_id = row["CrewId"]
            user_doc_id = row["UserDocId"]

            search_pattern = f"{employee_id}{user_doc_id}".strip().lower()
            match_row = db.crewing_employeemedicalcertificate.find_first(where={"EmployeeId": employee_id})

            if not match_row or not match_row.AttachmentFileId:
                print(f"No medSertifRow found for EmployeeId {employee_id}")
                continue

            medSertif_attachment = match_row.AttachmentFileId
            destination_path = os.path.join(destination_root, medSertif_attachment)
            # if os.path.exists(destination_path):
            #     shutil.rmtree(destination_path)

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
