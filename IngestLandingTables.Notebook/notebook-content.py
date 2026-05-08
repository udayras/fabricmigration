# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "0de2b9f1-5b15-4b4b-aebc-1d2b86f7b516",
# META       "default_lakehouse_name": "landing",
# META       "default_lakehouse_workspace_id": "4cf7fd12-8501-4181-9ebf-9c749bdea4dd",
# META       "known_lakehouses": [
# META         {
# META           "id": "0de2b9f1-5b15-4b4b-aebc-1d2b86f7b516"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "known_warehouses": []
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

employee_json = """
[
  {
    "employment": {
      "role": "Software Engineer",
      "startDate": "2021-01-15",
      "status": {
        "active": "true",
        "lastReview": "2023-07-01"
      }
    },
    "id": "E001",
    "profile": {
      "department": {
        "location": {
          "building": "A",
          "floor": "3"
        },
        "name": "Engineering"
      },
      "email": "alice@example.com",
      "name": {
        "first": "Alice",
        "last": "Smith"
      }
    }
  },
  {
    "employment": {
      "role": "Data Analyst",
      "startDate": "2022-03-10",
      "status": {
        "active": "true",
        "lastReview": "2023-05-15"
      }
    },
    "id": "E002",
    "profile": {
      "department": {
        "location": {
          "building": "B",
          "floor": "5"
        },
        "name": "Analytics"
      },
      "email": "bob@example.com",
      "name": {
        "first": "Bob",
        "last": "Johnson"
      }
    }
  }
]
"""

# Create an RDD from the JSON string
rdd = spark.sparkContext.parallelize([employee_json])

# Read JSON into DataFrame
df = spark.read.json(rdd)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_with_audit = df \
    .withColumn("created_on", current_timestamp()) \
    .withColumn("created_by", lit(JOB_NAME))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

variables = get_variables()
workspace_id = variables.process_workspace_id
student_lh_id = variables.lh_student_landing_id
schema_name = "dbo"
table_name = "employee"
abfss_path = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{student_lh_id}/Tables/{schema_name}/{table_name}"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_with_audit.write.mode("overwrite").save(abfss_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
