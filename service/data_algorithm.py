from service.column_mapping import REGISTERCREW_MAPPING, REGISTERCREW_ADDITIONAL


def transform_data(dataframe):
    # Rename columns sesuai mapping, abaikan kolom yang tidak di-map
    mapping = {k: v for k, v in REGISTERCREW_MAPPING.items() if v is not None}
    dataframe = dataframe.rename(columns=mapping)

    # Tambahkan kolom baru dengan nilai default dari REGISTERCREW_ADDITIONAL
    for col, value in REGISTERCREW_ADDITIONAL.items():
        if col not in dataframe.columns:
            dataframe[col] = value

    return dataframe
