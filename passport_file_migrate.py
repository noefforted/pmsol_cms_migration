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

        target_doc_type = "PASPORT"
        filtered_doc_ids = df1[df1["DocName"] == target_doc_type]["DocId"].tolist()

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
            match_row = db.crewing_employee.find_first(where={"Id": employee_id})

            if not match_row or not match_row.PassportPhotoId:
                print(f"No Passport record found for EmployeeId {employee_id}")
                continue

            passport_attachment = match_row.PassportPhotoId
            destination_path = os.path.join(destination_root, passport_attachment)
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
