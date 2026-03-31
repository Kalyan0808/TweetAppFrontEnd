timestamp_cols = ["crt_ts", "upd_ts"]   # <-- you define this

import pandas as pd
from pyspark.sql import functions as F

prd = pd.read_excel("prd_full.xlsx")
df = pd.read_excel("final_comparison.xlsx")

missing_df = df[df["status"] == "MISSING_IN_DEV"]

dev_catalog = "edl_dev"

timestamp_cols = ["crt_ts", "upd_ts"]   # 👈 YOU DEFINE

for _, row in missing_df.iterrows():
    
    schema = row["tgt_sys"]
    appl_id = row["appl_cntrl_id"]
    
    # Get full PRD row
    prd_row = prd[
        (prd["tgt_sys"] == schema) &
        (prd["appl_cntrl_id"] == appl_id)
    ]
    
    if prd_row.empty:
        continue
    
    insert_df = spark.createDataFrame(prd_row)
    
    try:
        target_table = f"{dev_catalog}.{schema}.appl_cntrl_log_l1"
        
        target_df = spark.table(target_table)
        target_cols = target_df.columns
        
        # Align columns
        insert_df = insert_df.select(*target_cols)
        
        # Convert to Spark DF with timestamp handling
        for col in timestamp_cols:
            if col in insert_df.columns:
                insert_df = insert_df.withColumn(col, F.current_timestamp())
        
        # Insert
        insert_df.write.mode("append").saveAsTable(target_table)
        
        print(f"Inserted: {schema} - {appl_id}")
    
    except Exception as e:
        print(f"Skipped {schema} - {appl_id} - {e}")
