# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "bfb20937-cff9-4e5d-a39d-ed1120f36104",
# META       "default_lakehouse_name": "bronze",
# META       "default_lakehouse_workspace_id": "4cf7fd12-8501-4181-9ebf-9c749bdea4dd",
# META       "known_lakehouses": [
# META         {
# META           "id": "4f904717-bb91-453b-b266-0e8eec0401c9"
# META         },
# META         {
# META           "id": "bfb20937-cff9-4e5d-a39d-ed1120f36104"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

%run get_env

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


from pyspark.sql.functions import current_timestamp, lit
from delta.tables import DeltaTable

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

variables = get_variables()
workspace_id = variables.process_workspace_id
lh_student_bronze_id = variables.lh_student_bronze_id
lh_student_silver_id = variables.lh_student_silver_id

schema_name = "dbo"
table_name = "employee"
bronze_path = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lh_student_bronze_id}/Tables/{schema_name}/{table_name}"
silver_path = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lh_student_silver_id}/Tables/{schema_name}/{table_name}"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze_lake_name = "bronze"
bronze_schema_name = "dbo"
brz_employee_table_name = f"{bronze_lake_name}.{bronze_schema_name}.employee"


silver_lake_name = "silver"
silver_schema_name = "dbo"
slvr_employee_table_name = f"{silver_lake_name}.{silver_schema_name}.employee"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def add_scd_type2_audit_columns(df):
    columns = {
        "effective_from": current_timestamp(),
        "effective_to": lit("9999-12-31 23:59:59").cast("timestamp"),
        "is_current": lit(1)
    }
    return df.withColumns(columns)

def perform_scd_type2(updates_df, target_path, merge_keys):


    target_table = DeltaTable.forPath(spark, target_path)
    audit_cols = ["effective_from", "effective_to", "is_current"]
    cols_to_exclude = audit_cols + merge_keys
    target_cols = [col for col in target_table.toDF().columns if col not in cols_to_exclude]
    update_check_cols = list(map(lambda x: f"(NOT (updates.{x} <=> target.{x}))", target_cols))
    update_check_str = f"AND ({' OR '.join(update_check_cols)})"


    new_records_to_insert = updates_df\
                    .alias("updates")\
                    .join(target_table.toDF().alias("target"), merge_keys)\
                    .where(f"target.is_current = 1 {update_check_str}")\
                    .select("updates.*")


    merge_pkeys_select = list(map(lambda x: f"NULL AS {x}_merge_key", merge_keys))
    updates_pkeys_select = list(map(lambda x: f"{x} AS {x}_merge_key", merge_keys))

    staged_updates = new_records_to_insert\
                    .selectExpr(*merge_pkeys_select, "*")\
                    .union(updates_df.selectExpr(*updates_pkeys_select, "*"))

    merge_key_condition = " AND ".join(list(map(lambda x: f"target.{x} = {x}_merge_key", merge_keys)))
    insert_dict = {f"target.{col}": col for col in target_table.toDF().columns}

    # Apply SCD Type 2 operation using merge
    target_table.alias("target").merge(
    staged_updates.alias("updates"),
    merge_key_condition) \
    .whenMatchedUpdate(
    condition = f"target.is_current = 1 {update_check_str}",
    set = {                                      # Set current to false and endDate to source's effective date.
        "is_current": lit(0),
        "effective_to": current_timestamp()
    }
    ).whenNotMatchedInsert(
    values = insert_dict
    ).execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze_df = spark.read.format("delta").load(bronze_path)\
        .drop("created_on", "created_by")

audit_col_added_df = add_scd_type2_audit_columns(bronze_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if DeltaTable.isDeltaTable(spark, silver_path):
    perform_scd_type2(audit_col_added_df, silver_path, ["id"])
else:
    audit_col_added_df.write.format("delta")\
        .save(silver_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
