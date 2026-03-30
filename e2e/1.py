from pyspark.sql import functions as F
import pandas as pd

schemas = ["schema1", "schema2", "schema3"]  # all 50
catalog = "edl_dev"

df_list = []

for schema in schemas:
    table_path = f"{catalog}.{schema}.appl_cntrl_log_l1"
    
    try:
        df = spark.table(table_path).select(
            F.col("tgt_sys"),
            F.col("tgt_obj"),
            F.col("load_nam")
        )
        df_list.append(df)
    except:
        print(f"Skipping {table_path}")

final_df = df_list[0].unionByName(*df_list[1:]) if df_list else None

pdf = final_df.toPandas()
pdf.to_excel("/dbfs/FileStore/dev_data.xlsx", index=False)

print("DEV file created")
