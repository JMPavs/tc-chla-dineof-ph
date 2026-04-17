import pandas as pd
from pathlib import Path
import sys

# Ensure your script can find the generator file
sys.path.append(r"D:\Thesis_2") 
from validation_summary_generator import generate_validation_summary

# ====================================================
# CONFIGURATION
# ====================================================
DIR_NORMAL = r"D:\Thesis_2\Output_Normal_3"
DIR_ANOM   = r"D:\Thesis_2\Output_Anomalous_3"
DIR_OUTPUT = r"D:\Thesis_2"  # Where to save the merged file

print("🚀 STARTING BATCH PROCESSING...\n")

# ====================================================
# 1. OUTPUT NORMAL SUMMARY
# ====================================================
print(f"--- Generating Output 1: Normal Summary ---")
df_normal = generate_validation_summary(DIR_NORMAL)
# Add label for merging later
if df_normal is not None:
    df_normal['Group'] = 'Normal'
print(f"✅ Normal Summary Created at: {DIR_NORMAL}\\validation_summary_all_TCs.csv\n")

# ====================================================
# 2. OUTPUT ANOMALOUS SUMMARY
# ====================================================
print(f"--- Generating Output 2: Anomalous Summary ---")
df_anom = generate_validation_summary(DIR_ANOM)
# Add label for merging later
if df_anom is not None:
    df_anom['Group'] = 'Anomalous'
print(f"✅ Anomalous Summary Created at: {DIR_ANOM}\\validation_summary_all_TCs.csv\n")

# ====================================================
# 3. OUTPUT MERGED SUMMARY
# ====================================================
print(f"--- Generating Output 3: Merged Summary ---")

if df_normal is not None and df_anom is not None:
    # Merge the two DataFrames
    df_merged = pd.concat([df_normal, df_anom], ignore_index=True)
    
    # Save the merged file
    merged_path = Path(DIR_OUTPUT) / "Validation_Summary_Merged_All.csv"
    df_merged.to_csv(merged_path, index=False)
    
    print(f"✅ MERGED Summary Created at: {merged_path}")
    print(f"   Total Records: {len(df_merged)} (Normal: {len(df_normal)}, Anomalous: {len(df_anom)})")
else:
    print("❌ Error: Could not merge because one or both datasets are empty.")

print("\nDONE! All 3 outputs are ready.")