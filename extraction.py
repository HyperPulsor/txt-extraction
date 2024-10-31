import dask.dataframe as dd
import pandas as pd
import os
import time

# 0.0001665 second for 1 row
input_folder = 'input'
output_folder = 'output'

os.makedirs(output_folder, exist_ok=True)

column_types = {
    "FORM_NO": str,
    "SANDI_PELAPOR": int,
    "FORM_PERIOD": int,
    "RECORD_NO": int,
    "NEGARA_ASAL": str,
    "KOTA_ASAL": int,
    "KOTA_TUJUAN": int,
    "NEGARA_TUJUAN": str,
    "NAMA_PENERIMA": str,
    "NAMA_PENGIRIM": str,
    "FREKUENSI_PENGIRIMAN": int,
    "FREKUENSI": int,
    "NOMINAL_TRX": int,
    "TUJUAN": int,
    "TUJUAN_TRX": int,
    "CREATED_DATE": str,
}

base_extract_cols = ["FORM_NO", "SANDI_PELAPOR", "FORM_PERIOD", "RECORD_NO", "NAMA_PENERIMA", "NAMA_PENGIRIM",
                     "FREKUENSI_PENGIRIMAN", "NOMINAL_TRX", "TUJUAN_TRX", "CREATED_DATE"]

form_types = {
    1: "Incoming",
    2: "Outgoing",
    3: "Domestik"
}

max_rows_per_sheet = 1_048_576

for file_name in os.listdir(input_folder):
    if file_name.endswith('.txt'):
        start_time = time.time()

        extract_cols = base_extract_cols.copy()
        while True:
            try:
                print(f"\nSelect Form Type for file {file_name}:")
                for key, value in form_types.items():
                    print(f"{key}. {value}")

                ans = int(input("Enter Form Type (1, 2, or 3): ").strip())

                if ans in form_types:
                    form_type = form_types[ans]
                    print(f"Processing {file_name} as '{form_type}' form.\n")
                    break
                else:
                    print("Invalid selection. Please enter a number between 1 and 3.")
            except ValueError:
                print("Invalid input. Please enter an integer between 1 and 3.")

        input_path = os.path.join(input_folder, file_name)
        output_file_name = os.path.splitext(file_name)[0] + '.xlsx'
        output_path = os.path.join(output_folder, output_file_name)

        record_no_index = extract_cols.index("RECORD_NO") + 1

        if ans == 1:
            extract_cols.insert(record_no_index, "KOTA_ASAL")
            extract_cols.insert(record_no_index + 1, "NEGARA_TUJUAN")
        elif ans == 2:
            if "TUJUAN_TRX" in extract_cols:
                extract_cols.remove("TUJUAN_TRX")
            if "FREKUENSI_PENGIRIMAN" in extract_cols:
                frekuensi_index = extract_cols.index("FREKUENSI_PENGIRIMAN")
                extract_cols[frekuensi_index] = "FREKUENSI"
            extract_cols.insert(record_no_index, "NEGARA_ASAL")
            extract_cols.insert(record_no_index + 1, "KOTA_TUJUAN")
        else:
            extract_cols.insert(record_no_index, "KOTA_ASAL")
            extract_cols.insert(record_no_index + 1, "KOTA_TUJUAN")

        try:
            # Ngebaca dulu filenya txt
            chunk_size = 1000000
            data = dd.read_csv(input_path, delimiter="|", names=extract_cols, dtype=column_types,
                               header=0, engine="pyarrow", encoding="latin-1", blocksize="512MB")

            data["CREATED_DATE"] = dd.to_datetime(data["CREATED_DATE"], format="%d-%m-%Y %H:%M:%S")
            data["CREATED_DATE"] = data["CREATED_DATE"].dt.strftime("%d/%m/%Y %H:%M:%S")

            with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
                sheet_index = 1
                for i, partition in enumerate(data.to_delayed()):
                    partition_df = partition.compute()

                    for start_row in range(0, len(partition_df), max_rows_per_sheet):
                        end_row = start_row + max_rows_per_sheet
                        sheet_df = partition_df.iloc[start_row:end_row]
                        sheet_name = f'Sheet_{sheet_index}'
                        sheet_df.to_excel(writer, sheet_name=sheet_name, index=False)
                        sheet_index += 1
                    del partition_df

            print(f"Data from {file_name} has been saved to {output_file_name}")
        except ValueError as ve:
            print(ve)
            print("Please Select the right Type of Form!\n")

        end_time = time.time()
        duration = end_time - start_time
        print(f"Processing time for {file_name}: {duration:.2f} seconds")

print("All files have been processed successfully.")