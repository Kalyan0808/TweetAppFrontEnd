schemas = ["schema1", "schema2", "schema3"]
catalog = "edl_dev"

df_list = []

for schema in schemas:
    try:
        df = spark.table(f"{catalog}.{schema}.appl_cntrl_log_l1")
        df_list.append(df)
    except:
        print(f"Skipping {schema}")

final_df = df_list[0].unionByName(*df_list[1:])

final_df.toPandas().to_excel("/dbfs/FileStore/dev_full.xlsx", index=False)
