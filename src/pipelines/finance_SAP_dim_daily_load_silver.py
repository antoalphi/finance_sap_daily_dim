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

RUN_ID = datetime.now().strftime("%Y%m%d_%H%M%S")
logger.info(f"[RUN_ID={RUN_ID}] Silver Pipeline started")

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

    Why this is important?
    - SAP extracted CSV files often have inconsistent headers.
    - Some columns may contain spaces, '-', '/', special characters.
    - Silver layer should have consistent naming standard.

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

    Why this is important?
    - Bronze layer is raw ingestion (header names may not be clean).
    - Silver layer must follow strict naming conventions.
    - Makes downstream joins and reporting consistent.
    """
    for c in df.columns:
        df = df.withColumnRenamed(c, standardize_column_name(c))
    return df


def trim_all_string_columns(df):
    """
    Removes unwanted leading/trailing spaces from ALL string columns.

    Why this is important?
    - SAP CSV files sometimes contain values like 'ABC ' or ' ABC'
    - Without trimming, joins may fail.
    - Example: 'A01' != 'A01 ' (this creates data quality issues)
    """
    for c, t in df.dtypes:
        if t == "string":
            df = df.withColumn(c, trim(col(c)))
    return df


def drop_metadata_columns(df):
    """
    Drops ingestion metadata columns from Bronze layer.

    Why this is important?
    - Bronze has columns like _ingestion_time, _load_date, _pipeline_run_id, etc.
    - These are useful in Bronze but not required in Silver output.
    - Silver should contain only business columns + minimal audit columns.
    """
    meta_cols = [c for c in df.columns if c.startswith("_")]
    return df.drop(*meta_cols)


def apply_casting_rules(df, casting_rules: dict):
    """
    Applies datatype casting rules on selected columns.

    Why this is important?
    - Bronze uses inferSchema, which may infer wrong types.
    - Silver must have stable datatypes for analytics and joins.

    Example:
      year should always be int,
      amount should always be decimal,
      date should always be date type.
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


def get_bad_records(df, required_cols: list):
    """
    Returns records where required columns are missing (NULL or blank).
    These will go into Quarantine table.
    """
    condition = None
    for c in required_cols:
        if c in df.columns:
            expr = (col(c).isNull()) | (trim(col(c)) == "")
            condition = expr if condition is None else (condition | expr)

    if condition is None:
        return df.limit(0)

    return df.filter(condition)


def get_good_records(df, required_cols: list):
    """
    Returns only valid records where required columns are not NULL or blank.
    These will go into Silver table.
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

# Reads config values injected from pipeline.yml (DLT configuration section)
cfg = get_config(spark, layer="silver")

# Captures job run id for audit purpose
pipeline_run_id = spark.conf.get("spark.databricks.job.runId", "unknown")

# Bronze schema should be passed via pipeline.yml configuration
# Default is bronze (safe fallback)
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

    # Quarantine table to store invalid records
    quarantine_table = f"{bronze_table}_quarantine"

    casting_rules = TABLE_CASTING_RULES.get(bronze_table, {})
    required_cols = TABLE_REQUIRED_COLUMNS.get(bronze_table, [])

    # Fully qualified bronze table reference
    # Example: demo_finance_sap.bronze.z_distch
    bronze_full_name = f"{cfg.catalog}.{bronze_schema}.{bronze_table}"


    # -------------------------------
    # Quarantine Table
    # -------------------------------
    @dlt.table(
        name=quarantine_table,
        comment=f"Quarantine table for bad records from {bronze_table}",
        table_properties={
            "quality": "quarantine",
            "layer": "silver",
            "source": "finance_sap_dimension"
        }
    )
    def build_quarantine(
        bronze_full_name=bronze_full_name,
        bronze_table=bronze_table,
        required_cols=required_cols,
        casting_rules=casting_rules,
        quarantine_table=quarantine_table
    ):

        # Logging (visible in DLT Event Logs)
        logger.info(f"Starting quarantine table build for {bronze_table}")
        logger.info(f"[SILVER-QUARANTINE] ---------------------------------------------")
        logger.info(f"[SILVER-QUARANTINE] Reading Bronze Table : {bronze_full_name}")
        logger.info(f"[SILVER-QUARANTINE] Writing Table       : {quarantine_table}")
        logger.info(f"[SILVER-QUARANTINE] Required Columns    : {required_cols}")
        logger.info(f"[SILVER-QUARANTINE] Casting Rules       : {casting_rules}")
        logger.info(f"[SILVER-QUARANTINE] Pipeline Run ID     : {pipeline_run_id}")

        # IMPORTANT:
        # Since Bronze and Silver are separate DLT pipelines,
        # we cannot use dlt.read(bronze_table).
        # We must read bronze using fully qualified name via spark.table().
        df = spark.table(bronze_full_name)

        # Step 1: Standardize column names into UPPER_CASE_WITH_UNDERSCORES
        # Example: "Day Suffix" -> "DAY_SUFFIX"
        df = standardize_all_column_names(df)

        # Step 2: Trim all string columns (remove leading/trailing spaces)
        df = trim_all_string_columns(df)

        # Step 3: Apply datatype casting rules (int/date/decimal conversions)
        df = apply_casting_rules(df, casting_rules)

        # Step 4: Filter out bad records (missing mandatory columns)
        bad_df = get_bad_records(df, required_cols)

        # Step 5: Add quarantine audit metadata
        bad_df = (
            bad_df.withColumn("_quarantine_reason", lit("Missing required column(s)"))
                  .withColumn("_quarantine_time", current_timestamp())
                  .withColumn("_silver_pipeline_run_id", lit(pipeline_run_id))
                  .withColumn("_source_bronze_table", lit(bronze_table))
        )

        return bad_df


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
        # This ensures consistency across all silver tables.
        df = standardize_all_column_names(df)

        # Step 2: Trim all string columns
        # Removes leading/trailing spaces so joins and comparisons work correctly.
        df = trim_all_string_columns(df)

        # Step 3: Apply datatype casting rules
        # Converts required fields into correct types (int/date/decimal).
        df = apply_casting_rules(df, casting_rules)

        # Step 4: Keep only good records
        # Filters out rows where mandatory business columns are missing.
        df = get_good_records(df, required_cols)

        # Step 5: Drop Bronze metadata columns (_ingestion_time, _load_date, etc.)
        # Silver should not carry Bronze-level ingestion metadata.
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
        # Ensures stable silver output if bronze has duplicates
        logger.info(f"[SILVER] Finished building dataframe for silver table: {silver_table}")
        return df.dropDuplicates()
