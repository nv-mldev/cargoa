import pandas as pd
import os
import re

# --- Configuration ---
INPUT_FILE = 'Chapter_1_-_Live_Animals.xlsx'
OUTPUT_DIR = "subheadings_excel"
HSN_COLUMN_INDEX = 0 

# --- Script Start ---

print(f"Processing '{INPUT_FILE}'...")
try:
    # 1. Read the Excel file
    df = pd.read_excel(INPUT_FILE, header=0, dtype={HSN_COLUMN_INDEX: str})
    hsn_col_name = df.columns[HSN_COLUMN_INDEX]
    df.rename(columns={hsn_col_name: 'HSN_CODE'}, inplace=True)
except FileNotFoundError:
    print(f"Error: The file '{INPUT_FILE}' was not found.")
    exit()
except Exception as e:
    print(f"An error occurred while reading the Excel file: {e}")
    exit()

# 2. Identify 4-digit subheadings and assign groups
df['HSN_CODE_CLEAN'] = df['HSN_CODE'].astype(str).fillna('').str.replace(r'\D', '', regex=True)
df['subheading_group'] = ''

current_subheading = None
for i, row in df.iterrows():
    hsn_code = row['HSN_CODE_CLEAN']
    # Check if the cleaned HSN code is exactly 4 digits
    if re.match(r'^\d{4}$', hsn_code):
        current_subheading = hsn_code
    
    # If we have found a subheading, assign it to the current row's group
    if current_subheading:
        df.at[i, 'subheading_group'] = current_subheading

# 3. Create a new Excel file for each unique subheading group
os.makedirs(OUTPUT_DIR, exist_ok=True)
print(f"Output directory '{OUTPUT_DIR}' created/ensured.")

unique_subheadings = df['subheading_group'].unique()
# Filter out any empty strings that might result from rows before the first 4-digit code
unique_subheadings = sorted([s for s in unique_subheadings if s])

if not unique_subheadings:
    print("No 4-digit HSN codes found in the file.")
    exit()
    
print(f"Found {len(unique_subheadings)} unique 4-digit HSN codes: {', '.join(unique_subheadings)}")

for subheading in unique_subheadings:
    # Filter the dataframe for the current subheading group
    df_filtered = df[df['subheading_group'] == subheading].copy()
    
    # Drop the helper columns before saving
    df_filtered.drop(columns=['HSN_CODE_CLEAN', 'subheading_group'], inplace=True)
    
    output_filename = os.path.join(OUTPUT_DIR, f"{subheading}.xlsx")
    
    df_filtered.to_excel(output_filename, index=False)
    print(f"  - Created '{output_filename}' with {len(df_filtered)} rows.")

print("\nProcessing complete.")