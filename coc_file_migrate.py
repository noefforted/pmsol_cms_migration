import os
import shutil
from prisma import Prisma
from repository.extract_repository import ExtractRepository

def copy_files_based_on_employee_and_userdoc():
    # Path sumber dan tujuan
    source_path = "/media/ahmadaufa/J Gab/FileUpload/F7PQ3azX7krRqesRDEYA/Documents/Sertifikat"
    destination_root = "/media/ahmadaufa/J Gab/SuperApp_Files_Fix/coc"

    # Inisialisasi Prisma
    db = Prisma()
    db.connect()

    try:
        # Ambil data dari ExtractRepository
        df1 = ExtractRepository.get_mstDoc()
        df2 = ExtractRepository.get_registerDoc()
        df2 = df2[df2["CrewId"].notnull() & (df2["CrewId"] != "")]

        # Filter df1 untuk DocType tertentu
        target_doc_type = "A12041A8-1A0F-4E01-A426-E1DBDF316613"
        filtered_doc_ids = df1[df1["DocType"] == target_doc_type]["DocId"].tolist()
        print(f"Filtered DocIds: {filtered_doc_ids}")

        if not filtered_doc_ids:
            print("No matching DocIds found in df1.")
            return

        # Filter df2 berdasarkan DocId
        filtered_df2 = df2[df2["DocId"].isin(filtered_doc_ids)]
        # print(f"Filtered df2: {filtered_df2}")

        if filtered_df2.empty:
            print("No matching rows found in df2.")
            return

        # Ambil data dari database Prisma

        # Iterasi setiap row dari filtered_df2
        for _, row in filtered_df2.iterrows():
            employee_id = row["CrewId"]
            user_doc_id = row["UserDocId"]

            search_pattern = f"{employee_id}{user_doc_id}"
            coc_row = db.crewing_employeecocdoc.find_first(where={"EmployeeId": employee_id})

            if not coc_row or not coc_row.COCAttachment:
                print(f"No COCRow found for EmployeeId {employee_id}")
                continue

            # print(f"Searching for {search_pattern} in {source_path}...")

            coc_attachment = coc_row.COCAttachment

            # Folder tujuan
            destination_path = os.path.join(destination_root, coc_attachment)
            os.makedirs(destination_path, exist_ok=True)

            # Normalisasi pola pencarian dan nama file
            search_pattern = search_pattern.strip().lower()

            for filename in os.listdir(source_path):
                filename_normalized = filename.strip().lower()
                
                if search_pattern in filename_normalized:
                    print(f"Found match: {filename}")
                    source_file = os.path.join(source_path, filename)
                    destination_file = os.path.join(destination_path, filename)
                    shutil.copy2(source_file, destination_file)
                    print(f"Copied {filename} to {destination_path}")

        print("All files copied successfully.")
    except Exception as e:
        print(f"Error during file copy: {e}")
    finally:
        db.disconnect()

# Panggil fungsi secara langsung
if __name__ == "__main__":
    copy_files_based_on_employee_and_userdoc()
