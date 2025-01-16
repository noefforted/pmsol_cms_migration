import os
from prisma import Prisma
from repository.extract_repository import ExtractRepository

def b_delete_files_based_on_employee_and_userdoc():
    # Path sumber dan tujuan
    destination_root = "/media/ahmadaufa/J Gab/SuperApp_Files_Fix/cop"

    # Inisialisasi Prisma
    db = Prisma()
    db.connect()

    try:
        # Ambil data dari ExtractRepository
        df1 = ExtractRepository.get_mstDoc()
        df2 = ExtractRepository.get_registerDoc()
        df2 = df2[df2["CrewId"].notnull() & (df2["CrewId"] != "")]

        # Ambil daftar DocId dari hasil filter
        filtered_doc_ids = df1[df1["DocName"] == 'MCU']["DocId"].tolist()
        print(f"Filtered DocIds: {filtered_doc_ids}")

        if not filtered_doc_ids:
            print("No matching DocIds found in df1.")
            return

        # Filter df2 berdasarkan DocId
        filtered_df2 = df2[df2["DocId"].isin(filtered_doc_ids)]

        if filtered_df2.empty:
            print("No matching rows found in df2.")
            return

        # Ambil data dari database Prisma
        # Iterasi setiap row dari filtered_df2
        for _, row in filtered_df2.iterrows():
            employee_id = row["CrewId"].lower()
            user_doc_id = row["UserDocId"].lower()

            cop_row = db.crewing_employeecopdoc.find_first(where={"EmployeeId": employee_id, "COPName": "MCU"})

            if not cop_row or not cop_row.COPAttachment:
                print(f"No COPRow found for EmployeeId {employee_id}")
                continue

            cop_attachment = cop_row.COPAttachment

            # Cari folder yang cocok
            for root, dirs, files in os.walk(destination_root):
                if os.path.basename(root) == cop_attachment:
                    target_substring = f"{employee_id}{user_doc_id}"

                    # Cari file yang mengandung substring target
                    for filename in files:
                        if target_substring in filename:
                            file_path = os.path.join(root, filename)
                            try:
                                os.remove(file_path)
                                print(f"Deleted file: {file_path}")
                            except Exception as e:
                                print(f"Error deleting file {file_path}: {e}")

        print("Specified files deleted successfully.")
    except Exception as e:
        print(f"Error during file deletion: {e}")
    finally:
        db.disconnect()

if __name__ == "__main__":
    b_delete_files_based_on_employee_and_userdoc()
