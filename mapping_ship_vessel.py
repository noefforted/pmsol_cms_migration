import pandas as pd

def create_mappingShipToVessel():
    # Membaca data dari file CSV
    ship_data = pd.read_csv("./data_source/cms_ship.csv")
    vessel_data = pd.read_csv("./data_source/nahkoda_vessel.csv")

    # Menampilkan kolom untuk debugging
    print("Kolom di ship_data:", ship_data.columns)
    print("Kolom di vessel_data:", vessel_data.columns)

    # Mengubah kolom menjadi lowercase dan menghilangkan spasi untuk perbandingan
    ship_data["ShipName_lower"] = ship_data["ShipName"].str.lower().str.replace(" ", "")
    vessel_data["VesselName_lower"] = vessel_data["VesselName"].str.lower().str.replace(" ", "")

    # Inisialisasi kolom baru di ship_data
    ship_data["VesselName"] = None
    ship_data["VesselId"] = None
    ship_data["VesselType"] = None

    # Periksa apakah ShipName_lower adalah substring dari VesselName_lower
    for i, ship_row in ship_data.iterrows():
        for j, vessel_row in vessel_data.iterrows():
            if ship_row["ShipName_lower"] in vessel_row["VesselName_lower"]:
                ship_data.at[i, "VesselName"] = vessel_row["VesselName"]
                ship_data.at[i, "VesselId"] = vessel_row["Id"]
                ship_data.at[i, "VesselType"] = vessel_row["VesselType"]
                break  # Keluar dari loop jika sudah cocok

    # Menghapus kolom sementara
    ship_data.drop(columns=["ShipName_lower"], inplace=True)

    # Menyimpan hasil ke file baru
    ship_data.to_csv("./data_source/updated_ship_data.csv", index=False)
    print("Data berhasil diperbarui dan disimpan ke 'updated_ship_data.csv'.")

# Memanggil fungsi
create_mappingShipToVessel()
