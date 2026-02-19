import datetime
import sys
import dlt
import re
import logging

from pyspark.sql.functions import (
    col,
    trim,
    current_timestamp,
    lit,
    to_date
)
from pyspark.sql.types import DecimalType, IntegerType
from config import get_config


# ------------------------------------------------------------------------------
# Logger Setup (Best Practice)
# ------------------------------------------------------------------------------
LOGGER_NAME = "finance_sap_dim_silver_pipeline"

logger = logging.getLogger(LOGGER_NAME)

if not logger.handlers:  # Prevent duplicate logs in notebook reruns
    logger.setLevel(logging.INFO)

    handler = logging.StreamHandler(sys.stdout)  # Databricks captures stdout logs
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )
    handler.setFormatter(formatter)

    logger.addHandler(handler)
 
logger.info("Silver Pipeline started")


# -------------------------------
# Helper Functions
# -------------------------------

def clean_table_name(folder_name: str) -> str:
    """
    Converts folder names like 'Z_DISTCH' or 'z-distch'
    into a valid Delta table name like 'z_distch'.

    NOTE:
    - Table names are kept lowercase because Databricks table naming convention
      usually uses lowercase.
    """
    name = re.sub(r"[^a-zA-Z0-9_]", "_", folder_name)
    name = re.sub(r"_+", "_", name)
    return name.lower()


def standardize_column_name(col_name: str) -> str:
    """
    Converts column name into UPPER_CASE_WITH_UNDERSCORES.

    Examples:
      'Material Code '  -> 'MATERIAL_CODE'
      'Profit-Center'   -> 'PROFIT_CENTER'
      'Day Suffix'      -> 'DAY_SUFFIX'
    """
    col_name = col_name.strip()
    col_name = col_name.upper()

    # Replace all non-alphanumeric characters with underscore
    col_name = re.sub(r"[^A-Z0-9]+", "_", col_name)

    # Replace multiple underscores with a single underscore
    col_name = re.sub(r"_+", "_", col_name)

    # Remove leading/trailing underscores
    return col_name.strip("_")


def standardize_all_column_names(df):
    """
    Renames ALL columns in the dataframe into UPPER_CASE_WITH_UNDERSCORES format.
    """
    for c in df.columns:
        df = df.withColumnRenamed(c, standardize_column_name(c))
    return df


def trim_all_string_columns(df):
    """
    Removes unwanted leading/trailing spaces from ALL string columns.
    """
    for c, t in df.dtypes:
        if t == "string":
            df = df.withColumn(c, trim(col(c)))
    return df


def drop_metadata_columns(df):
    """
    Drops ingestion metadata columns from Bronze layer.
    Example: _rescued_data, _ingestion_time, etc.
    """
    meta_cols = [c for c in df.columns if c.startswith("_")]
    return df.drop(*meta_cols)


def apply_casting_rules(df, casting_rules: dict):
    """
    Applies datatype casting rules on selected columns.
    """

    for c, dtype in casting_rules.items():
        if c in df.columns:

            if dtype == "int":
                df = df.withColumn(c, col(c).cast(IntegerType()))

            elif dtype.startswith("decimal"):
                # Example: decimal(18,2)
                m = re.match(r"decimal\((\d+),(\d+)\)", dtype)
                if m:
                    p, s = int(m.group(1)), int(m.group(2))
                    df = df.withColumn(c, col(c).cast(DecimalType(p, s)))

            elif dtype == "date":
                # Converts yyyy-MM-dd into date
                df = df.withColumn(c, to_date(col(c), "yyyy-MM-dd"))

            elif dtype == "date_ddmmyyyy":
                # Converts dd.MM.yyyy into date (SAP format)
                df = df.withColumn(c, to_date(col(c), "dd.MM.yyyy"))

            else:
                # Generic casting fallback
                df = df.withColumn(c, col(c).cast(dtype))

    return df


def get_good_records(df, required_cols: list):
    """
    Returns only valid records where required columns are not NULL or blank.
    """
    condition = None
    for c in required_cols:
        if c in df.columns:
            expr = (col(c).isNotNull()) & (trim(col(c)) != "")
            condition = expr if condition is None else (condition & expr)

    if condition is None:
        return df

    return df.filter(condition)


# -------------------------------
# Config
# -------------------------------

cfg = get_config(spark, layer="silver")

pipeline_run_id = spark.conf.get("spark.databricks.job.runId", "unknown")

bronze_schema = spark.conf.get("finance_SAP_dim.schema.bronze", "bronze")


# -------------------------------
# Casting Rules (Customize per table)
# -------------------------------

TABLE_CASTING_RULES = {
    "z_date": {
        "DATE": "date",
        "YEAR": "int",
        "MONTH": "int",
        "DAY": "int"
    }
}

# IMPORTANT:
# Since we standardize column names into UPPER_CASE,
# required column names must also be in UPPER_CASE.
TABLE_REQUIRED_COLUMNS = {
    "z_material": ["MATERIAL"],
    "z_prfcent": ["PROFIT_CENTER"],
    "z_distch": ["DISTRIBUTION_CHANNEL"]
}


# -------------------------------
# Dynamic Silver Tables
# -------------------------------

for folder_name in cfg.z_tables:

    folder_name = folder_name.strip()

    bronze_table = clean_table_name(folder_name)

    # Silver table name is same as bronze (recommended for easy usage)
    silver_table = f"{bronze_table}"

    casting_rules = TABLE_CASTING_RULES.get(bronze_table, {})
    required_cols = TABLE_REQUIRED_COLUMNS.get(bronze_table, [])

    # Fully qualified bronze table reference
    bronze_full_name = f"{cfg.catalog}.{bronze_schema}.{bronze_table}"


    # -------------------------------
    # Silver Table
    # -------------------------------
    @dlt.table(
        name=silver_table,
        comment=f"Silver cleaned dimension table derived from {bronze_table}",
        table_properties={
            "quality": "silver",
            "layer": "silver",
            "source": "finance_sap_dimension",
            "project": "Finance SAP dim daily load - silver",
        },
    )
    def build_silver(
        bronze_full_name=bronze_full_name,
        bronze_table=bronze_table,
        required_cols=required_cols,
        casting_rules=casting_rules,
        silver_table=silver_table
    ):

        # Logging (visible in DLT Event Logs)
        logger.info(f"[SILVER] --------------------------------------------------")
        logger.info(f"[SILVER] Reading Bronze Table : {bronze_full_name}")
        logger.info(f"[SILVER] Writing Silver Table : {silver_table}")
        logger.info(f"[SILVER] Required Columns     : {required_cols}")
        logger.info(f"[SILVER] Casting Rules        : {casting_rules}")
        logger.info(f"[SILVER] Pipeline Run ID      : {pipeline_run_id}")

        # Read the bronze table (cross-pipeline read)
        df = spark.table(bronze_full_name)

        # -------------------------------
        # Core Silver Transformations
        # -------------------------------

        # Step 1: Standardize column names into UPPER_CASE_WITH_UNDERSCORES
        df = standardize_all_column_names(df)

        # Step 2: Trim all string columns
        df = trim_all_string_columns(df)

        # Step 3: Apply datatype casting rules
        df = apply_casting_rules(df, casting_rules)

        # Step 4: Keep only good records (mandatory columns check)
        df = get_good_records(df, required_cols)

        # Step 5: Drop Bronze metadata columns (_rescued_data, _ingestion_time, etc.)
        df = drop_metadata_columns(df)

        # -------------------------------
        # Silver Audit Columns
        # -------------------------------
        df = (
            df.withColumn("_silver_process_time", current_timestamp())
              .withColumn("_silver_pipeline_run_id", lit(pipeline_run_id))
              .withColumn("_source_bronze_table", lit(bronze_table))
        )

        # Step 6: Remove duplicates
        df = df.dropDuplicates()

        logger.info(f"[SILVER] Finished building dataframe for silver table: {silver_table}")

        return df
