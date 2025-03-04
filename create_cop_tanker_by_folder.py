import os
from prisma import Prisma
import uuid
from datetime import datetime

# Inisialisasi Prisma client
prisma = Prisma()

def insert_employee_cop_docs(base_folder):
    # Membuka koneksi ke database
    prisma.connect()
    
    # Iterasi seluruh folder dalam base_folder
    for full_name in os.listdir(base_folder):
        full_name_path = os.path.join(base_folder, full_name, "cop")
        
        if os.path.isdir(full_name_path):
            # Normalisasi nama folder FullName dengan mengubah menjadi lowercase dan menghilangkan spasi
            normalized_full_name = full_name.lower().replace(" ", "")
            print(f"Processing folder: {full_name} (Normalized: {normalized_full_name})")
            
            # Mendapatkan semua Employee dari database untuk pencopokan
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
                
                # Iterasi subfolder yang berisi copName
                for cop_name in os.listdir(full_name_path):
                    cop_name_path = os.path.join(full_name_path, cop_name)
                    
                    if os.path.isdir(cop_name_path):
                        # Normalisasi copName dengan mengubah menjadi lowercase dan menghilangkan spasi
                        normalized_cop_name = cop_name.lower()
                        print(f"Processing cop folder: {cop_name} (Normalized: {normalized_cop_name})")
                        
                        # Mendapatkan copId berdasarkan copName yang sudah dinormalisasi
                        cop = prisma.crewing_coptanker.find_first(where={"COPName": normalized_cop_name})
                        
                        if cop:
                            cop_id = cop.COPId
                            print(f"Found cop: {cop_name} (copId: {cop_id})")
                            
                            # Insert ke Crewing_EmployeecopDoc
                            prisma.crewing_employeecopdoc.create(
                                data={
                                    'Id': str(uuid.uuid4()),
                                    'EmployeeId': employee_id,
                                    'COPName': cop_name,
                                    'COPId': cop_id,
                                    'COPAttachment': str(uuid.uuid4()),
                                    'Created': datetime.now(),
                                    'CreatedBy': 'Migration',
                                    'IsDeleted': False,
                                }
                            )
                            print(f"Inserted copDoc for employee {employee_id}: {cop_name}")
                        else:
                            print(f"cop not found for {cop_name}")
            else:
                print(f"Employee with FullName {full_name} not found.")
    
    # Menutup koneksi
    prisma.disconnect()
    print("Finished processing all folders.")

# Menjalankan fungsi
base_folder = "/home/ahmadaufa/Downloads/Ready_all"  # Ganti dengan path folder yang sesuai
insert_employee_cop_docs(base_folder)
