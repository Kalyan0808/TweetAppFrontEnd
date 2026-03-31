import pandas as pd

dev = pd.read_excel("dev_full.xlsx")
prd = pd.read_excel("prd_full.xlsx")

# Only use required cols for comparison
compare_df = dev.merge(
    prd,
    on=["tgt_sys", "tgt_obj"],
    how="outer",
    suffixes=("_dev", "_prd")
)

def get_status(row):
    if pd.isna(row["load_nam_prd"]):
        return "EXTRA_IN_DEV"
    elif pd.isna(row["load_nam_dev"]):
        return "MISSING_IN_DEV"
    elif row["load_nam_dev"] != row["load_nam_prd"]:
        return "WORKFLOW_MISMATCH"
    else:
        return "MATCH"

compare_df["status"] = compare_df.apply(get_status, axis=1)

final_df = compare_df[compare_df["status"] != "MATCH"]

final_df.to_excel("final_comparison.xlsx", index=False)
