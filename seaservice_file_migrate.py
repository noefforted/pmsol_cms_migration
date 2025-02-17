import os
import shutil
import fitz  # PyMuPDF
from prisma import Prisma
from repository.extract_repository import ExtractRepository

def merge_pdfs(pdf_list, output_path):
    """Menggabungkan beberapa file PDF menjadi satu file."""
    if not pdf_list:
        print("No PDFs to merge.")
        return None

    merged_pdf = fitz.open()
    
    for pdf in pdf_list:
        try:
            with fitz.open(pdf) as doc:
                if doc.page_count > 0:  # Cek apakah PDF memiliki halaman
                    merged_pdf.insert_pdf(doc)
                else:
                    print(f"Skipping empty PDF: {pdf}")
        except Exception as e:
            print(f"Skipping corrupted or unreadable PDF: {pdf} - Error: {e}")
            continue  # Skip ke file berikutnya jika ada error

    if merged_pdf.page_count == 0:
        print("Merged PDF has zero pages. Skipping save.")
        return None  # Tidak menyimpan file kosong

    try:
        merged_pdf.save(output_path)
        merged_pdf.close()
        return output_path
    except Exception as e:
        print(f"Error saving merged PDF: {output_path} - {e}")
        return None

def copy_and_merge_latest_files():
    source_path = "/media/ahmadaufa/J Gab/FileUpload/F7PQ3azX7krRqesRDEYA/Documents/Sertifikat"
    destination_root = "/media/ahmadaufa/J Gab/NAHKODA_Files/seaservice"

    db = Prisma()
    db.connect()

    try:
        df1 = ExtractRepository.get_mstDoc()
        df2 = ExtractRepository.get_registerDoc()
        df2 = df2[df2["CrewId"].notnull() & (df2["CrewId"] != "")]

        target_doc_type = "SEA MAN BOOK"
        filtered_doc_ids = df1[df1["DocName"] == target_doc_type]["DocId"].tolist()

        if not filtered_doc_ids:
            print("No matching DocIds found in df1.")
            return

        filtered_df2 = df2[df2["DocId"].isin(filtered_doc_ids)]

        if filtered_df2.empty:
            print("No matching rows found in df2.")
            return
        
        # Cek folder yang sudah ada sebelum iterasi
        existing_folders = set(os.listdir(destination_root))
        
        for _, row in filtered_df2.iterrows():
            employee_id = row["CrewId"]
            user_doc_id = row["UserDocId"]

            search_pattern = f"{employee_id}{user_doc_id}".strip().lower()
            match_row = db.crewing_employeeseaservice.find_first(where={"EmployeeId": employee_id})

            if not match_row or not match_row.SeaServiceAttachment:
                print(f"No SeaService record found for EmployeeId {employee_id}")
                continue

            seaservice_attachment = match_row.SeaServiceAttachment
            destination_path = os.path.join(destination_root, seaservice_attachment)
      
            # Skip jika folder sudah ada
            if seaservice_attachment in existing_folders:
                print(f"Skipping existing folder: {destination_path}")
                continue
            
            os.makedirs(destination_path, exist_ok=True)

            matched_files = [
                os.path.join(source_path, f)
                for f in os.listdir(source_path)
                if search_pattern in f.lower() and f.endswith(".pdf")  # Hanya file PDF
            ]

            if not matched_files:
                print(f"No matching PDFs found for {search_pattern}, skipping...")
                continue  # Lewati jika tidak ada file yang cocok

            merged_pdf_path = os.path.join(destination_path, f"{search_pattern}_merged.pdf")

            # Jika hanya ada 1 file, langsung copy tanpa merge
            if len(matched_files) == 1:
                try:
                    shutil.copy2(matched_files[0], merged_pdf_path)
                    print(f"Copied single file {matched_files[0]} to {merged_pdf_path}")
                except Exception as e:
                    print(f"Error copying file {matched_files[0]}: {e}")
                continue

            # Jika ada lebih dari 1 file, lakukan merge
            merged_pdf = merge_pdfs(matched_files, merged_pdf_path)
            if merged_pdf:
                print(f"Merged and copied PDF to {merged_pdf_path}")
            else:
                print(f"Skipping merge for {search_pattern} due to errors.")

        print("All matching files merged and copied successfully.")
    except Exception as e:
        print(f"Error during file copy process: {e}")
    finally:
        db.disconnect()

if __name__ == "__main__":
    copy_and_merge_latest_files()