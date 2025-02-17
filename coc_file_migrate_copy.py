import os
import shutil
from prisma import Prisma
from repository.extract_repository import ExtractRepository

COCuuid_to_docName_mapping = {
    "DD9AB9B9-E4F7-4174-80A2-3CC04D961A42": "ENDORSEMENT",
    "A80303C3-56E5-4463-96C6-F46B62D1F849": "Ijazah (tipe ijazah tidak diketahui)",
    "A80303C3-56E5-4463-96C6-F46B62D1F849CCD231DF-51D6-4F57-B9A4-E7AC91DAE096": "RASD",
    "A80303C3-56E5-4463-96C6-F46B62D1F84914E1BD41-BF82-4BEF-9102-7558FF91EFF0": "DOC I",
    "A80303C3-56E5-4463-96C6-F46B62D1F8495200306F-B1AC-4DEB-92BE-D2EE3273C34B": "DOC II",
    "A80303C3-56E5-4463-96C6-F46B62D1F849E479D469-C902-4944-815F-78F443B8AD2F": "DOC III",
    "A80303C3-56E5-4463-96C6-F46B62D1F849BC36EF55-61B7-46D2-9149-2EBE416907DE": "DOC IV",
    "A80303C3-56E5-4463-96C6-F46B62D1F849DEC6B749-F9DA-47CC-8E3C-37EFCC01E0B4": "DOC V",
    "A80303C3-56E5-4463-96C6-F46B62D1F84946926539-9F5D-4040-ACF4-025FC066C2F3": "RASE",
    "A80303C3-56E5-4463-96C6-F46B62D1F849B1D8F01F-AD1C-4451-BCF9-31641B5D8621": "EOC I",
    "A80303C3-56E5-4463-96C6-F46B62D1F8491241A596-2A01-4DFB-B3E2-E12434752944": "EOC II",
    "A80303C3-56E5-4463-96C6-F46B62D1F8490BD4BAAB-C009-44F4-8267-E8DCE4F4D381": "EOC III",
    "A80303C3-56E5-4463-96C6-F46B62D1F849CE4C72A8-0391-4E5A-8F44-135C3B6A5F69": "EOC IV",
    "A80303C3-56E5-4463-96C6-F46B62D1F8493BAD689A-3B60-46FA-BE2E-3D09AF6EA892": "EOC V",
    "A80303C3-56E5-4463-96C6-F46B62D1F849A912D717-24F2-4B69-AB98-BAA2778E8022": "RASD",
    "A80303C3-56E5-4463-96C6-F46B62D1F849C545718B-8001-43F2-BF41-6DDFD246BB0C": "RASE"
}

def copy_latest_file():
    source_path = "/media/ahmadaufa/J Gab/FileUpload/F7PQ3azX7krRqesRDEYA/Documents/Sertifikat"
    destination_root = "/media/ahmadaufa/J Gab/NAHKODA_Files/coc"

    db = Prisma()
    db.connect()

    try:
        df1 = ExtractRepository.get_mstDoc()
        df2 = ExtractRepository.get_registerDoc()
        df2 = df2[df2["CrewId"].notnull() & (df2["CrewId"] != "")]

        target_doc_type = "A12041A8-1A0F-4E01-A426-E1DBDF316613"
        filtered_doc_ids = df1[df1["DocType"] == target_doc_type]["DocId"].tolist()

        if not filtered_doc_ids:
            print("No matching DocIds found in df1.")
            return

        filtered_df2 = df2[df2["DocId"].isin(filtered_doc_ids)]

        if filtered_df2.empty:
            print("No matching rows found in df2.")
            return

        for _, row in filtered_df2.iterrows():
            employee_id = row["CrewId"]
            user_doc_id = row["UserDocId"]
            doc_id = row["DocId"]
            ijasah_id = row["IjasahId"]
            ijasah_name = COCuuid_to_docName_mapping.get(f"{doc_id}{ijasah_id}", None)

            match_row = db.crewing_employeecocdoc.find_first(
                where={"EmployeeId": employee_id, "COCName": ijasah_name}
            )

            if not match_row or not match_row.COCAttachment:
                print(f"No COCRow found for EmployeeId {employee_id}")
                continue

            COC_attachment = match_row.COCAttachment
            destination_path = os.path.join(destination_root, COC_attachment)
            os.makedirs(destination_path, exist_ok=True)

            search_pattern = f"{employee_id}{user_doc_id}".strip().lower()
            matched_files = [
                os.path.join(source_path, f)
                for f in os.listdir(source_path)
                if search_pattern in f.lower()
            ]

            if matched_files:
                latest_file = max(matched_files, key=os.path.getmtime)
                destination_file = os.path.join(destination_path, os.path.basename(latest_file))
                shutil.copy2(latest_file, destination_file)
                print(f"Copied latest file {latest_file} to {destination_path}")

        print("Latest files copied successfully.")
    except Exception as e:
        print(f"Error during file copy: {e}")
    finally:
        db.disconnect()

if __name__ == "__main__":
    copy_latest_file()
