import pandas as pd

# Load full extracts
dev = pd.read_excel("dev_full.xlsx")
prd = pd.read_excel("prd_full.xlsx")

# Merge using correct composite key
df = prd.merge(
    dev,
    on=["tgt_sys", "appl_cntrl_id"],
    how="outer",
    suffixes=("_prd", "_dev"),
    indicator=True
)

# Status logic (PRD is source of truth)
def get_status(row):
    if row["_merge"] == "left_only":
        return "MISSING_IN_DEV"
    elif row["_merge"] == "right_only":
        return "EXTRA_IN_DEV"
    elif row["load_nam_prd"] != row["load_nam_dev"]:
        return "WORKFLOW_MISMATCH"
    else:
        return "MATCH"

df["status"] = df.apply(get_status, axis=1)

# Keep only issues
final_df = df[df["status"] != "MATCH"]

final_df.to_excel("final_comparison.xlsx", index=False)

print("Comparison done")
