import uuid
from prisma import Prisma

def update_regphotoid():
    # Inisialisasi koneksi Prisma
    db = Prisma()
    db.connect()
    
    try:
        # Ambil semua baris dari tabel crewing_employee
        rows = db.crewing_employee.find_many()
        
        # Iterasi setiap baris untuk memperbarui semua baris di crewing_employeecocdoc
        for row in rows:
            new_uuid = row.PhotoId  # Ambil UUID baru dari crewing_employee
            db.crewing_employeemedicalcertificate.update_many(
                where={"EmployeeId": row.Id},  # Update semua baris dengan EmployeeId yang cocok
                data={"AttachmentFileId": new_uuid},
            )
            print(f"Updated all COCAttachment for EmployeeId {row.Id} to {new_uuid}")
        
        print(f"Updated all rows successfully.")
    except Exception as e:
        print(f"Error updating RegPhotoId: {e}")
    finally:
        # Tutup koneksi Prisma
        db.disconnect()

# Panggil fungsi secara langsung
if __name__ == "__main__":
    update_regphotoid()
