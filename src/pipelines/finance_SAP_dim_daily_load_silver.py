import sys
import dlt
import re
import logging

from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col,
    trim,
    current_timestamp,
    lit,
    to_date,
    row_number
)
from pyspark.sql.types import DecimalType, IntegerType

from config import get_config
from table_rules_config import DEDUP_KEYS, REQUIRED_COLUMNS


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

logger.info("Finance SAP Dimension Silver Pipeline started")


# -------------------------------
# Helper Functions
# -------------------------------

def clean_table_name(folder_name: str) -> str:
    """
    Converts folder names like 'Z_DISTCH' or 'z-distch'
    into a valid Delta table name like 'z_distch'.
    """
    name = re.sub(r"[^a-zA-Z0-9_]", "_", folder_name)
    name = re.sub(r"_+", "_", name)
    return name.lower()


def standardize_column_name(col_name: str) -> str:
    """
    Converts column name into UPPER_CASE_WITH_UNDERSCORES.
    """
    col_name = col_name.strip().upper()
    col_name = re.sub(r"[^A-Z0-9]+", "_", col_name)
    col_name = re.sub(r"_+", "_", col_name)
    return col_name.strip("_")


def standardize_all_column_names(df):
    """
    Renames ALL columns into UPPER_CASE_WITH_UNDERSCORES format.
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
                m = re.match(r"decimal\((\d+),(\d+)\)", dtype)
                if m:
                    p, s = int(m.group(1)), int(m.group(2))
                    df = df.withColumn(c, col(c).cast(DecimalType(p, s)))

            elif dtype == "date":
                df = df.withColumn(c, to_date(col(c), "yyyy-MM-dd"))

            elif dtype == "date_ddmmyyyy":
                df = df.withColumn(c, to_date(col(c), "dd.MM.yyyy"))

            else:
                df = df.withColumn(c, col(c).cast(dtype))

    return df


def split_required_good_bad(df, required_cols: list):
    """
    Splits dataframe into valid and invalid records based on required column rules.
    Bad record = required column is NULL or blank.
    """
    if not required_cols:
        empty_bad = spark.createDataFrame([], df.schema)
        return df, empty_bad

    condition = None
    for c in required_cols:
        if c in df.columns:
            expr = (col(c).isNotNull()) & (trim(col(c)) != "")
            condition = expr if condition is None else (condition & expr)

    if condition is None:
        empty_bad = spark.createDataFrame([], df.schema)
        return df, empty_bad

    valid_df = df.filter(condition)
    invalid_df = df.filter(~condition)

    return valid_df, invalid_df


def split_duplicates(df, table_name: str):
    """
    Splits dataframe into unique records and duplicates based on configured DEDUP_KEYS.
    Keeps first record and moves remaining duplicates to duplicate_df.
    """
    keys = DEDUP_KEYS.get(table_name)

    if not keys:
        empty_dups = spark.createDataFrame([], df.schema)
        return df.dropDuplicates(), empty_dups

    window_spec = Window.partitionBy(*keys).orderBy(lit(1))

    ranked_df = df.withColumn("_row_num", row_number().over(window_spec))

    unique_df = ranked_df.filter(col("_row_num") == 1).drop("_row_num")
    duplicate_df = ranked_df.filter(col("_row_num") > 1).drop("_row_num")

    return unique_df, duplicate_df


# -------------------------------
# Config
# -------------------------------
cfg = get_config(spark, layer="silver")

pipeline_run_id = spark.conf.get("spark.databricks.job.runId", "unknown")
bronze_schema = spark.conf.get("finance_SAP_dim.schema.bronze", "bronze")


# -------------------------------
# Casting Rules (Customize per table), This is incomplete, need to add the casting rules for all tables and columns based on the actual data types in source and desired types in silver.
# -------------------------------
TABLE_CASTING_RULES = {
    "z_date": {
        "DATE": "date",
        "YEAR": "int",
        "MONTH": "int",
        "DAY": "int"
    }
}


# ------------------------------------------------------------------------------
# ONE COMMON QUARANTINE TABLE (Bad Records + Duplicates from all Z tables)
# ------------------------------------------------------------------------------
@dlt.table(
    name="finance_sap_dim_quarantine",
    comment="Central quarantine table storing rejected and duplicate records from Finance SAP dimension silver pipeline",
    table_properties={
        "quality": "quarantine",
        "layer": "silver",
        "source": "finance_sap_dimension",
        "project": "Finance SAP dim daily load - quarantine"
    },
)
def finance_sap_dim_quarantine():

    quarantine_union_df = None

    for folder_name in cfg.z_tables:

        folder_name = folder_name.strip()
        bronze_table = clean_table_name(folder_name)

        required_cols = REQUIRED_COLUMNS.get(bronze_table, [])
        casting_rules = TABLE_CASTING_RULES.get(bronze_table, {})

        bronze_full_name = f"{cfg.catalog}.{bronze_schema}.{bronze_table}"

        logger.info(f"[QUARANTINE] Processing Bronze Table : {bronze_full_name}")

        df = spark.table(bronze_full_name)

        # Apply common standardization steps
        df = standardize_all_column_names(df)
        df = trim_all_string_columns(df)
        df = apply_casting_rules(df, casting_rules)

        # -------------------------------
        # Split mandatory column failures
        # -------------------------------
        valid_df, bad_df = split_required_good_bad(df, required_cols)

        bad_df = (
            bad_df.withColumn("_quarantine_time", current_timestamp())
                  .withColumn("_quarantine_type", lit("BAD_RECORD"))
                  .withColumn("_quarantine_reason", lit("MANDATORY_COLUMNS_MISSING_OR_BLANK"))
                  .withColumn("_missing_required_cols", lit(",".join(required_cols)))
                  .withColumn("_silver_pipeline_run_id", lit(pipeline_run_id))
                  .withColumn("_source_bronze_table", lit(bronze_table))
        )

        # -------------------------------
        # Split duplicates (only from valid records)
        # -------------------------------
        unique_df, duplicate_df = split_duplicates(valid_df, bronze_table)

        duplicate_df = (
            duplicate_df.withColumn("_quarantine_time", current_timestamp())
                        .withColumn("_quarantine_type", lit("DUPLICATE"))
                        .withColumn("_quarantine_reason", lit("DUPLICATE_RECORD_FOUND"))
                        .withColumn("_missing_required_cols", lit(None))
                        .withColumn("_silver_pipeline_run_id", lit(pipeline_run_id))
                        .withColumn("_source_bronze_table", lit(bronze_table))
        )

        # Drop bronze metadata columns
        bad_df = drop_metadata_columns(bad_df)
        duplicate_df = drop_metadata_columns(duplicate_df)

        # Union all quarantine records
        combined_quarantine_df = bad_df.unionByName(duplicate_df, allowMissingColumns=True)

        quarantine_union_df = (
            combined_quarantine_df if quarantine_union_df is None
            else quarantine_union_df.unionByName(combined_quarantine_df, allowMissingColumns=True)
        )

    # Safety fallback
    if quarantine_union_df is None:
        quarantine_union_df = spark.createDataFrame([], schema="")

    return quarantine_union_df


# ------------------------------------------------------------------------------
# Dynamic Silver Tables
# ------------------------------------------------------------------------------
for folder_name in cfg.z_tables:

    folder_name = folder_name.strip()

    bronze_table = clean_table_name(folder_name)
    silver_table = bronze_table

    casting_rules = TABLE_CASTING_RULES.get(bronze_table, {})
    required_cols = REQUIRED_COLUMNS.get(bronze_table, [])

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

        logger.info(f"[SILVER] --------------------------------------------------")
        logger.info(f"[SILVER] Reading Bronze Table : {bronze_full_name}")
        logger.info(f"[SILVER] Writing Silver Table : {silver_table}")
        logger.info(f"[SILVER] Required Columns     : {required_cols}")
        logger.info(f"[SILVER] Casting Rules        : {casting_rules}")
        logger.info(f"[SILVER] Pipeline Run ID      : {pipeline_run_id}")

        df = spark.table(bronze_full_name)

        # Step 1: Standardize column names
        df = standardize_all_column_names(df)

        # Step 2: Trim string columns
        df = trim_all_string_columns(df)

        # Step 3: Apply datatype casting rules
        df = apply_casting_rules(df, casting_rules)

        # Step 4: Remove bad records (mandatory cols missing)
        valid_df, _ = split_required_good_bad(df, required_cols)

        # Step 5: Remove duplicates and keep only unique records
        final_silver_df, _ = split_duplicates(valid_df, bronze_table)

        # Step 6: Drop bronze metadata columns
        final_silver_df = drop_metadata_columns(final_silver_df)

        # Step 7: Add Silver audit columns
        final_silver_df = (
            final_silver_df.withColumn("_silver_process_time", current_timestamp())
                           .withColumn("_silver_pipeline_run_id", lit(pipeline_run_id))
                           .withColumn("_source_bronze_table", lit(bronze_table))
        )

        logger.info(f"[SILVER] Finished building silver table: {silver_table}")

        return final_silver_df
