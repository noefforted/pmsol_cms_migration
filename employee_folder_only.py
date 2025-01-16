import os
import shutil  # Untuk menghapus folder
from prisma import Prisma

def create_folders_for_photoid(base_directory):
    # Inisialisasi koneksi Prisma
    db = Prisma()
    db.connect()

    try:
        # Ambil semua baris dengan PhotoId yang tidak kosong
        rows = db.crewing_employee.find_many()

        # Ambil daftar PhotoId dari database
        photo_ids = {row.PhotoId for row in rows if row.PhotoId is not None}

        # Buat folder untuk setiap PhotoId
        for photo_id in photo_ids:
            folder_path = os.path.join(base_directory, photo_id)

            # Buat folder jika belum ada
            if not os.path.exists(folder_path):
                os.makedirs(folder_path)
                print(f"Created folder: {folder_path}")
            else:
                print(f"Folder already exists: {folder_path}")

        # Periksa folder yang ada di base_directory
        existing_folders = {name for name in os.listdir(base_directory) if os.path.isdir(os.path.join(base_directory, name))}

        # Temukan folder yang tidak ada di database dan hapus
        folders_to_delete = existing_folders - photo_ids
        for folder_name in folders_to_delete:
            folder_path = os.path.join(base_directory, folder_name)
            shutil.rmtree(folder_path)  # Menghapus folder beserta isinya
            print(f"Deleted folder: {folder_path}")

        print(f"Successfully synced folders with {len(photo_ids)} PhotoId values.")
    except Exception as e:
        print(f"Error syncing folders: {e}")
    finally:
        # Tutup koneksi Prisma
        db.disconnect()

# Panggil fungsi ini
if __name__ == "__main__":
    base_directory = "/media/ahmadaufa/J Gab/SuperApp_Files_Fix/personaldata"  # Ganti dengan path folder utama Anda
    create_folders_for_photoid(base_directory)
