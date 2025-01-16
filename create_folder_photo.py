import os
from prisma import Prisma

def create_folders_for_photoid(base_directory):
    # Inisialisasi koneksi Prisma
    db = Prisma()
    db.connect()

    try:
        # Ambil semua baris dengan PhotoId yang tidak kosong
        rows = db.crewing_employee.find_many()

        # Iterasi setiap baris untuk membuat folder
        for row in rows:
            folder_name = row.PhotoId
            if folder_name is None:
                print("Skipping entry with None as PhotoId")
                continue
            folder_path = os.path.join(base_directory, folder_name)

            # Buat folder jika belum ada
            if not os.path.exists(folder_path):
                os.makedirs(folder_path)
                print(f"Created folder: {folder_path}")
            else:
                print(f"Folder already exists: {folder_path}")

        print(f"Successfully created folders for {len(rows)} PhotoId values.")
    except Exception as e:
        print(f"Error creating folders: {e}")
    finally:
        # Tutup koneksi Prisma
        db.disconnect()

# Panggil fungsi ini
if __name__ == "__main__":
    base_directory = "/media/ahmadaufa/J Gab/SuperApp_Files_Fix/cop"  # Ganti dengan path folder utama Anda
    create_folders_for_photoid(base_directory)
