import pandas as pd
from pyspark.sql import functions as F

# Load full datasets
dev = pd.read_excel("dev_full.xlsx")
prd = pd.read_excel("prd_full.xlsx")
cmp = pd.read_excel("final_comparison.xlsx")

# Filter only required fixes
cmp = cmp[cmp["status"].isin(["MISSING_IN_DEV", "WORKFLOW_MISMATCH"])]

# Get required records from PRD FULL data
keys = cmp[["tgt_sys", "tgt_obj"]]

prd_filtered = prd.merge(keys, on=["tgt_sys", "tgt_obj"], how="inner")

# Convert to Spark DF
prd_spark_df = spark.createDataFrame(prd_filtered)

# Create temp view
prd_spark_df.createOrReplaceTempView("prd_updates")

# --------------------------------------------------
# APPLY MERGE PER SCHEMA
# --------------------------------------------------

catalog = "edl_dev"

schemas = prd_filtered["tgt_sys"].unique()

for schema in schemas:
    
    table_path = f"{catalog}.{schema}.appl_cntrl_log_l1"
    
    merge_sql = f"""
    MERGE INTO {table_path} AS dev
    USING (
        SELECT * FROM prd_updates WHERE tgt_sys = '{schema}'
    ) AS prd
    ON dev.tgt_sys = prd.tgt_sys
       AND dev.tgt_obj = prd.tgt_obj
    
    WHEN MATCHED THEN UPDATE SET *
    
    WHEN NOT MATCHED THEN INSERT *
    """
    
    spark.sql(merge_sql)
    
    print(f"Synced schema: {schema}")
