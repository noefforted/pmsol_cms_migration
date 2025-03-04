import os
import shutil
from prisma import Prisma

# Fungsi untuk mendapatkan data crew dari database
def get_crew_tanker():
    prisma = Prisma()
    prisma.connect()

    # Mengambil data crew berdasarkan FullName dan KtpPhotoId
    crew_tanker = prisma.crewing_employee.find_many(where={"JobPositionTankerId": {"not": None},
                                                           "PhotoId": {"not": None}})

    prisma.disconnect()
    return crew_tanker

# Fungsi untuk membuat folder berdasarkan KtpPhotoId dan menyalin foto
def create_folder_and_copy_photos(crew_tanker, source_base_dir, destination_base_dir):
    for crew in crew_tanker:
        # Mendapatkan nama crew dan KtpPhotoId
        full_name = (crew.FullName).lower()
        photoKtp_id = crew.KtpPhotoId

        # Path folder baru yang akan dibuat berdasarkan KtpPhotoId
        new_folder_path = os.path.join(destination_base_dir, photoKtp_id)

        # Membuat folder jika belum ada
        os.makedirs(new_folder_path, exist_ok=True)

        # Path sumber foto berdasarkan struktur folder yang diberikan
        source_photo_path = os.path.join(source_base_dir, full_name.title(), "personaldata", "ktp")
        
        # Mengecek apakah folder sumber ada
        if os.path.exists(source_photo_path):
            # Menyalin foto pertama yang ditemukan (asumsi hanya ada satu foto per crew)
            for file_name in os.listdir(source_photo_path):
                source_file = os.path.join(source_photo_path, file_name)
                if os.path.isfile(source_file):
                    # Menyalin file foto ke dalam folder tujuan berdasarkan KtpPhotoId
                    shutil.copy(source_file, os.path.join(new_folder_path, file_name))
                    print(f"Foto {file_name} disalin ke {new_folder_path}")
                    break  # Hanya salin satu file foto per crew
        else:
            print(f"Folder foto untuk {full_name} tidak ditemukan di {source_photo_path}")

# Fungsi utama yang menggabungkan semua proses
def main():
    # Ambil data crew dari database
    crew_tanker = get_crew_tanker()

    # Tentukan direktori sumber dan tujuan
    source_base_dir = '/home/ahmadaufa/Downloads/Ready_all'
    destination_base_dir = '/media/ahmadaufa/J Gab/NAKHODA_Files_Tanker/personaldata'

    # Buat folder dan salin foto
    create_folder_and_copy_photos(crew_tanker, source_base_dir, destination_base_dir)

# Jalankan script utama
if __name__ == "__main__":
    main()
