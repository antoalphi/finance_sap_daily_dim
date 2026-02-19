import datetime
import logging
import sys
import dlt
import re

from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    current_timestamp,
    current_date,
    lit,
    col
)

from config import get_config

# ------------------------------------------------------------------------------
# Logger Setup (Best Practice)
# ------------------------------------------------------------------------------
LOGGER_NAME = "finance_sap_dim_bronze_pipeline"

logger = logging.getLogger(LOGGER_NAME)

if not logger.handlers:  # Prevent duplicate logs in notebook reruns
    logger.setLevel(logging.INFO)

    handler = logging.StreamHandler(sys.stdout)  # Databricks captures stdout logs
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )
    handler.setFormatter(formatter)

    logger.addHandler(handler)

 
logger.info("Bronze Pipeline started")
 
# -------------------------------
# Helper Functions
# -------------------------------

def clean_table_name(folder_name: str) -> str:
    """
    Cleans folder name into a valid Delta table name.

    Example:
      "Z_DISTCH" -> "z_distch"
      "z-distch" -> "z_distch"
    """
    name = re.sub(r"[^a-zA-Z0-9_]", "_", folder_name)
    name = re.sub(r"_+", "_", name)
    return name.lower()


def list_csv_files(folder_path: str):
    """
    Lists only CSV files from the given landing folder path.

    Why needed?
    - Landing folder may contain _SUCCESS or other unwanted files
    - We want to ingest only .csv files
    """
    files = dbutils.fs.ls(folder_path)
    return [f.path for f in files if f.path.lower().endswith(".csv")]


def detect_delimiter(file_path: str) -> str:
    """
    Detect delimiter from first line (header) of the CSV file.

    Why needed?
    - SAP extracts may come with different separators like:
      comma (,), semicolon (;), pipe (|), tab (\t)
    - This logic ensures ingestion works even if delimiter changes.
    """

    # Reads the first 4096 bytes from the file header
    # Default is 1024 but not enough for long SAP headers
    sample = dbutils.fs.head(file_path, 4096)

    candidates = [",", ";", "|", "\t"]
    best_delim = ","
    best_count = 0

    for d in candidates:
        count = sample.count(d)
        if count > best_count:
            best_count = count
            best_delim = d

    return best_delim


# -------------------------------
# Config
# -------------------------------

# Reads values injected from pipeline.yml into spark.conf
cfg = get_config(spark, layer="bronze")

# Captures pipeline/job run id (useful for audit tracking)
pipeline_run_id = spark.conf.get("spark.databricks.job.runId", "unknown")


# -------------------------------
# Dynamic Bronze Table Creation
# -------------------------------

# Loop through configured Z tables (passed from bundle config)
for folder_name in cfg.z_tables:

    folder_name = folder_name.strip()

    # Bronze table name should match folder name (cleaned)
    table_name = clean_table_name(folder_name)

    # Landing path example:
    # abfss://.../landing/dim/z_distch/
    folder_path = f"{cfg.landing_path}/{folder_name}/"

    @dlt.table(
        name=table_name,
        comment=f"Bronze Delta table created from landing folder {folder_name}",
        table_properties={
            "quality": "bronze",
            "layer": "bronze",
            "source": "finance_sap_dimension",
            "project": "Finance SAP dim daily load",
        },
    )
    def load_table(folder_path=folder_path, folder_name=folder_name, table_name=table_name):

        # Logging - shows in DLT event logs
        logger.info(f"[BRONZE] Processing folder: {folder_name}")
        logger.info(f"[BRONZE] Landing folder path: {folder_path}")
        logger.info(f"[BRONZE] Target bronze table name: {table_name}")

        # Get list of all CSV files from landing folder
        csv_files = list_csv_files(folder_path)

        logger.info(f"[BRONZE] Number of CSV files found: {len(csv_files)}")

        # If folder is empty, return an empty dataframe to avoid pipeline failure
        # This is important because schema inference fails on empty reads
        if not csv_files:
            logger.warning(f"[BRONZE] No CSV files found in {folder_path}. Returning empty dataframe.")
            return spark.createDataFrame([], schema="dummy STRING").limit(0)

        dfs = []

        # Loop through each CSV file and load it
        for file_path in csv_files:

            # Detect delimiter dynamically
            delim = detect_delimiter(file_path)

            logger.info(f"[BRONZE] Reading file: {file_path}")
            logger.info(f"[BRONZE] Detected delimiter: '{delim}'")

            # Read CSV file
            # inferSchema is okay for Bronze, because Bronze is raw ingestion
            df = (
                spark.read.format("csv")
                .option("header", "true")
                .option("sep", delim)
                .option("inferSchema", "true")
                .option("includeMetadata", "true")
                .load(file_path)
            )

            # Add ingestion metadata columns
            # These are useful for traceability and debugging
            df = (
                df.withColumn("_ingestion_time", current_timestamp())         # when record was loaded
                  .withColumn("_load_date", current_date())                  # date partitioning support
                  .withColumn("_pipeline_run_id", lit(pipeline_run_id))      # identifies job run
                  .withColumn("_source_file", col("_metadata.file_path"))    # file path of source
                  .withColumn("_source_folder", lit(folder_name))            # folder name of source
                  .withColumn("_delimiter_used", lit(delim))                 # delimiter used for read
            )

            dfs.append(df)

        # Union all files into one dataframe
        # unionByName ensures correct mapping even if column order differs between files
        logger.info(f"[BRONZE] Unioning {len(dfs)} dataframe(s) into final bronze table: {table_name}")
        final_df = reduce(DataFrame.unionByName, dfs)

        # NOTE (Optional Enhancement):
        # If you want to track uniqueness / change detection, you can compute a record hash
        # record_hash = sha2(concat_ws("||", *final_df.columns), 256)
        # final_df = final_df.withColumn("_record_hash", record_hash)

        logger.info(f"[BRONZE] Finished building dataframe for bronze table: {table_name}")

        return final_df
