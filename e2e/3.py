import pandas as pd

df = pd.read_excel("final_comparison.xlsx")

mismatch_df = df[df["status"] == "WORKFLOW_MISMATCH"]

dev_catalog = "edl_dev"

for _, row in mismatch_df.iterrows():
    schema = row["tgt_sys"]
    appl_id = row["appl_cntrl_id"]
    correct_load = row["load_nam_prd"]
    
    query = f"""
    UPDATE {dev_catalog}.{schema}.appl_cntrl_log_l1
    SET load_nam = '{correct_load}'
    WHERE appl_cntrl_id = '{appl_id}'
      AND tgt_sys = '{schema}'
    """
    
    spark.sql(query)
    print(f"Updated: {schema} - {appl_id}")
