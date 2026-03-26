from pyspark.sql import functions as F
import pandas as pd

# Load files
dev = pd.read_excel("dev_data.xlsx")
prd = pd.read_excel("prd_data.xlsx")

# --------------------------------------------------
# 1. TABLE LEVEL COMPARISON (PRD as source of truth)
# --------------------------------------------------

df = dev.merge(
    prd,
    on=["tgt_sys", "tgt_obj"],
    how="outer",
    suffixes=("_dev", "_prd")
)

# Add status column
def get_status(row):
    if pd.isna(row["load_nam_prd"]):
        return "EXTRA_IN_DEV"   # not expected
    elif pd.isna(row["load_nam_dev"]):
        return "MISSING_IN_DEV" # must fix
    elif row["load_nam_dev"] != row["load_nam_prd"]:
        return "WORKFLOW_MISMATCH"
    else:
        return "MATCH"

df["status"] = df.apply(get_status, axis=1)

# Keep only issues (optional: remove MATCH)
table_mismatch_df = df[df["status"] != "MATCH"]

# --------------------------------------------------
# 2. WORKFLOW LEVEL VALIDATION
# --------------------------------------------------

workflow_results = []

prd_group = prd.groupby("load_nam")
dev_group = dev.groupby("load_nam")

all_workflows = set(prd["load_nam"]).union(set(dev["load_nam"]))

for wf in all_workflows:
    
    prd_tables = set()
    dev_tables = set()
    
    if wf in prd_group.groups:
        prd_tables = set(
            zip(
                prd_group.get_group(wf)["tgt_sys"],
                prd_group.get_group(wf)["tgt_obj"]
            )
        )
    
    if wf in dev_group.groups:
        dev_tables = set(
            zip(
                dev_group.get_group(wf)["tgt_sys"],
                dev_group.get_group(wf)["tgt_obj"]
            )
        )
    
    missing_in_dev = prd_tables - dev_tables
    extra_in_dev = dev_tables - prd_tables
    
    workflow_results.append({
        "load_nam": wf,
        "missing_tables_in_dev": len(missing_in_dev),
        "extra_tables_in_dev": len(extra_in_dev)
    })

workflow_df = pd.DataFrame(workflow_results)

# Keep only problematic workflows
workflow_df = workflow_df[
    (workflow_df["missing_tables_in_dev"] > 0) |
    (workflow_df["extra_tables_in_dev"] > 0)
]

# --------------------------------------------------
# 3. WRITE TO EXCEL (MULTIPLE SHEETS)
# --------------------------------------------------

output_file = "final_comparison.xlsx"

with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
    table_mismatch_df.to_excel(writer, sheet_name="Table_Level_Issues", index=False)
    workflow_df.to_excel(writer, sheet_name="Workflow_Level_Issues", index=False)

print("Final comparison file generated:", output_file)
