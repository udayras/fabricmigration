# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d0f18dda-1838-4668-a256-16f056f6ae2d",
# META       "default_lakehouse_name": "landing",
# META       "default_lakehouse_workspace_id": "45bb2834-8188-4831-9b42-099cdb0859e8",
# META       "known_lakehouses": [
# META         {
# META           "id": "d0f18dda-1838-4668-a256-16f056f6ae2d"
# META         },
# META         {
# META           "id": "0bf3f7b4-eeb4-4309-a890-39e19a5fe968"
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

from pyspark.sql.functions import struct, col, lit, current_timestamp

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

JOB_NAME = notebookutils.runtime.context['currentNotebookName']

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

variables = get_variables()
workspace_id = variables.process_workspace_id
lh_student_landing_id = variables.lh_student_landing_id
schema_name = "dbo"
table_name = "employee"
bronze_path = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lh_student_landing_id}/Tables/{schema_name}/{table_name}"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read from landing.dbo.employee
source_df = spark.read.format("delta")\
                .load(bronze_path)

flat_df = source_df.select(
    "id",
    "employment.role",
    "employment.startDate",
    "employment.status.active",
    "employment.status.lastReview",
    "profile.department.name",
    "profile.department.location.building",
    "profile.department.location.floor",
    "profile.email",
    col("profile.name.first").alias("first_name"),
    col("profile.name.last").alias("last_name"),
    lit("current_timstamp").alias("created_by"),
    lit(JOB_NAME).alias("created_on")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

lh_student_bronze_id = variables.lh_student_bronze_id
schema_name = "dbo"
table_name = "employee"
bronze_path = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lh_student_bronze_id}/Tables/{schema_name}/{table_name}"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

flat_df.write.mode("append").save(bronze_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
