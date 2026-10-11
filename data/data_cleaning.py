import os
import pandas as pd
import numpy as np
import glob

def clean_column_names(df):
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('/', '_').str.replace('-', '_')
    return df

def basic_data_cleaning(input_dir, output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    csv_files = glob.glob(os.path.join(input_dir, "*.csv"))
    
    for file_path in csv_files:
        file_name = os.path.basename(file_path)
        print(f"Processing: {file_name}")
        
        df = pd.read_csv(file_path, low_memory=False)
        original_shape = df.shape
        
        # 1. Chuẩn hóa tên cột
        df = clean_column_names(df)
        
        # 2. Xử lý Infinite values và Missing values
        df.replace([np.inf, -np.inf], np.nan, inplace=True)
        df.dropna(inplace=True)
        
        # 3. Sửa lỗi encoding kí tự rác
        if 'label' in df.columns:
            df['label'] = df['label'].astype(str).str.replace(r'[^\x00-\x7F]+', '-', regex=True)
            df['label'] = df['label'].str.replace(' - ', '-')
        
        # 4. Loại bỏ Duplicate records
        df.drop_duplicates(inplace=True)
        new_shape = df.shape
        
        output_path = os.path.join(output_dir, f"cleaned_{file_name}")
        df.to_csv(output_path, index=False)

if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    input_directory = os.path.join(script_dir, "raw_data")
    output_directory = os.path.join(script_dir, "cleaned_data")
    
    print(f"Reading raw data from: {input_directory}")
    print(f"Saving cleaned data to: {output_directory}")
    basic_data_cleaning(input_directory, output_directory)
    print("Hoàn tất Data Cleaning!")

