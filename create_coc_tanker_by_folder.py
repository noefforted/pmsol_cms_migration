import os
from prisma import Prisma
import uuid
from datetime import datetime

# Inisialisasi Prisma client
prisma = Prisma()

def insert_employee_coc_docs(base_folder):
    # Membuka koneksi ke database
    prisma.connect()
    
    # Iterasi seluruh folder dalam base_folder
    for full_name in os.listdir(base_folder):
        full_name_path = os.path.join(base_folder, full_name, "coc")
        
        if os.path.isdir(full_name_path):
            # Normalisasi nama folder FullName dengan mengubah menjadi lowercase dan menghilangkan spasi
            normalized_full_name = full_name.lower().replace(" ", "")
            print(f"Processing folder: {full_name} (Normalized: {normalized_full_name})")
            
            # Mendapatkan semua Employee dari database untuk pencocokan
            employees = prisma.crewing_employee.find_many(where={"JobPositionTankerId": {"not": None},
                                                           "PhotoId": {"not": None}})
            print(f"Total employees found: {len(employees)}")
            
            # Menemukan Employee yang FullName-nya sudah dinormalisasi
            employee = None
            for emp in employees:
                print(f"Checking employee: {emp.FullName}")
                if emp.FullName.lower().replace(" ", "") == normalized_full_name:
                    employee = emp
                    print(f"Employee matched: {emp.FullName} (ID: {emp.Id})")
                    break
            
            if employee:
                employee_id = employee.Id
                print(f"Found employee ID: {employee_id}")
                
                # Iterasi subfolder yang berisi COCName
                for coc_name in os.listdir(full_name_path):
                    coc_name_path = os.path.join(full_name_path, coc_name)
                    
                    if os.path.isdir(coc_name_path):
                        # Normalisasi COCName dengan mengubah menjadi lowercase dan menghilangkan spasi
                        normalized_coc_name = coc_name.lower()
                        print(f"Processing COC folder: {coc_name} (Normalized: {normalized_coc_name})")
                        
                        # Mendapatkan COCId berdasarkan COCName yang sudah dinormalisasi
                        coc = prisma.crewing_coctanker.find_first(where={"COCName": {"contains": normalized_coc_name}})
                        
                        if coc:
                            coc_id = coc.COCId
                            print(f"Found COC: {coc_name} (COCId: {coc_id})")
                            
                            # Insert ke Crewing_EmployeeCOCDoc
                            prisma.crewing_employeecocdoc.create(
                                data={
                                    'Id': str(uuid.uuid4()),
                                    'EmployeeId': employee_id,
                                    'COCName': coc_name,
                                    'COCId': coc_id,
                                    'COCAttachment': str(uuid.uuid4()),
                                    'Created': datetime.now(),
                                    'CreatedBy': 'Migration',
                                    'IsDeleted': False,
                                }
                            )
                            print(f"Inserted COCDoc for employee {employee_id}: {coc_name}")
                        else:
                            print(f"COC not found for {coc_name}")
            else:
                print(f"Employee with FullName {full_name} not found.")
    
    # Menutup koneksi
    prisma.disconnect()
    print("Finished processing all folders.")

# Menjalankan fungsi
base_folder = "/home/ahmadaufa/Downloads/Ready_all"  # Ganti dengan path folder yang sesuai
insert_employee_coc_docs(base_folder)
