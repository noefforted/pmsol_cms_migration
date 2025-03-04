import os
import shutil
from prisma import Prisma

# Fungsi untuk mendapatkan data dari Crewing_EmployeecopDoc berdasarkan filter tanggal
def get_new_cop_docs():
    prisma = Prisma()
    prisma.connect()

    # Mengambil data Crewing_EmployeecopDoc yang baru dibuat setelah 2025-02-25
    new_cop_docs = prisma.crewing_employeecopdoc.find_many(
        where={"Created": {"gt": "2025-02-25T00:00:00.000Z"}}
    )

    prisma.disconnect()
    return new_cop_docs

# Fungsi untuk membuat folder berdasarkan employeeId dan menyalin file dari folder copName
def copy_cop_files(new_cop_docs, source_base_dir, destination_base_dir):
    prisma = Prisma()
    prisma.connect()

    for doc in new_cop_docs:
        employee_id = doc.EmployeeId
        cop_name = doc.COPName
        cop_attachment = doc.COPAttachment

        # Mendapatkan FullName berdasarkan employeeId
        employee = prisma.crewing_employee.find_unique(where={"Id": employee_id})

        if employee:
            full_name = employee.FullName.title()  # Normalisasi FullName agar sesuai dengan folder
            source_folder_path = os.path.join(source_base_dir, full_name, "cop", cop_name)

            # Path folder tujuan berdasarkan employeeId
            destination_folder_path = os.path.join(destination_base_dir, cop_attachment)

            # Membuat folder tujuan jika belum ada
            os.makedirs(destination_folder_path, exist_ok=True)

            # Mengecek apakah folder sumber ada
            if os.path.exists(source_folder_path):
                # Menyalin semua file dalam folder sumber
                for file_name in os.listdir(source_folder_path):
                    source_file = os.path.join(source_folder_path, file_name)
                    if os.path.isfile(source_file):
                        shutil.copy(source_file, os.path.join(destination_folder_path, file_name))
                        print(f"File {file_name} disalin dari {source_file} ke {destination_folder_path}")
            else:
                print(f"Folder cop untuk {full_name} dengan copName '{cop_name}' tidak ditemukan di {source_folder_path}")

    prisma.disconnect()
    print("Proses penyalinan selesai.")

# Fungsi utama yang menggabungkan semua proses
def main():
    # Ambil data cop terbaru dari database
    new_cop_docs = get_new_cop_docs()

    # Tentukan direktori sumber dan tujuan
    source_base_dir = "/home/ahmadaufa/Downloads/Ready_all"
    destination_base_dir = "/media/ahmadaufa/J Gab/NAKHODA_Files_Tanker/cop"

    # Menyalin file dari folder copName ke tujuan
    copy_cop_files(new_cop_docs, source_base_dir, destination_base_dir)

# Jalankan script utama
if __name__ == "__main__":
    main()
