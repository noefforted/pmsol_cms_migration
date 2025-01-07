import os
import shutil
import pandas as pd
from repository.extract_repository import ExtractRepository
from prisma import Prisma

def copy_photo():
    prisma = Prisma()
    prisma.connect()
    try:
        # Ambil data dari ExtractRepository
        df1 = ExtractRepository.get_RegisterCrew()

        # Ambil data dari Prisma dan konversi menjadi DataFrame
        data = prisma.crewing_registercrew.find_many()
        df2 = pd.DataFrame([dict(item) for item in data])

        # Debug: Periksa kolom yang tersedia
        # print("DF1 Columns:", df1.columns)
        # print("DF2 Columns:", df2.columns)

        # Konversi kolom Id ke string dan normalkan ke huruf kecil
        df1['Id'] = df1['Id'].astype(str).str.lower().str.strip()
        df2['Id'] = df2['Id'].astype(str).str.lower().str.strip()

        # Base directory tempat folder UUID disimpan
        source_directory = "/media/ahmadaufa/J Gab/FileUpload/F7PQ3azX7krRqesRDEYA/Documents"

        # Iterasi berdasarkan kolom Id yang cocok
        for _, row1 in df1.iterrows():
            # Pastikan file_path tidak kosong
            file_path = f"{source_directory}{row1['Foto']}"
            file_path = file_path.replace("\\", "/")
            if not row1['Foto'] or pd.isna(file_path):
                print(f"Skipping row with empty Foto for Id: {row1['Id']}")
                continue
            # print(f"File Path: {file_path}")

            # Filter baris di df2 dengan Id yang sama
            matching_rows = df2[df2['Id'] == row1['Id']]
            
            if matching_rows.empty:
                print(f"No matching row found for Id: {row1['Id']}")
                continue

            # Ambil baris pertama dari hasil filter
            row2 = matching_rows.iloc[0]
            
            # Ambil informasi folder tujuan
            destination_base = "/media/ahmadaufa/J Gab/SuperApp_Files/personaldata"
            destination_folder = os.path.join(destination_base, row2['RegPhotoId'])
            destination_path = os.path.join(destination_folder, os.path.basename(file_path))
            
            # Pastikan folder tujuan ada
            if not os.path.exists(destination_folder):
                os.makedirs(destination_folder)
            
            # Copy file ke folder tujuan
            try:
                shutil.copy(file_path, destination_path)
                print(f"Copied {file_path} to {destination_path}")
            except Exception as e:
                print(f"Failed to copy {file_path} to {destination_path}: {e}")
    except Exception as e:
        print(f"Error copying photo: {e}")
    finally:
        prisma.disconnect()

if __name__ == "__main__":
    copy_photo()
