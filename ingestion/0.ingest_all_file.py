# Databricks notebook source
import concurrent.futures

# List of child notebook paths
child_notebook_paths = [
    "1. ingest_circuits_file",
    "2. ingest_races_file",
    "3. ingest_constructors_json_file",
    "4. ingest_driver_file",
    "5. ingest_results_json_file",
    "6. ingest_pitstops_mutiline_json_file",
    "7. ingest_lap_times_mutiple_csv_file",
    "8. ingest_qualifying_mutiline_multiple_json_file"
]

# Function to run a child notebook
def run_child_notebook(child_notebook_path):
    result = dbutils.notebook.run(child_notebook_path, 0, {'p_data_source':'Ergast API'})


# Run child notebooks in parallel
with concurrent.futures.ThreadPoolExecutor() as executor:
    futures = executor.map(run_child_notebook, child_notebook_paths)



# COMMAND ----------


