from pyspark.sql import functions as F
import pandas as pd

# Load exported files
dev = pd.read_excel("dev_data.xlsx")
prd = pd.read_excel("prd_data.xlsx")

# --------------------------------------------------
# TABLE LEVEL COMPARISON (PRD as source of truth)
# --------------------------------------------------

df = dev.merge(
    prd,
    on=["tgt_sys", "tgt_obj"],
    how="outer",
    suffixes=("_dev", "_prd")
)

# Status logic
def get_status(row):
    if pd.isna(row["load_nam_prd"]):
        return "EXTRA_IN_DEV"        # exists only in DEV (not expected)
    elif pd.isna(row["load_nam_dev"]):
        return "MISSING_IN_DEV"      # missing in DEV (must fix)
    elif row["load_nam_dev"] != row["load_nam_prd"]:
        return "WORKFLOW_MISMATCH"   # incorrect workflow
    else:
        return "MATCH"

df["status"] = df.apply(get_status, axis=1)

# Keep only issues
final_df = df[df["status"] != "MATCH"]

# --------------------------------------------------
# WRITE TO EXCEL
# --------------------------------------------------

output_file = "final_comparison.xlsx"
final_df.to_excel(output_file, index=False)

print("Final comparison file generated:", output_file)
