import dlt
import re
from pyspark.sql.functions import (
    col,
    trim,
    current_timestamp,
    lit,
    to_date
)
from pyspark.sql.types import DecimalType, IntegerType
from config import get_config


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
    Standardizes column names into snake_case.

    Example:
      'Material Code '  -> 'material_code'
      'Profit-Center'   -> 'profit_center'
    """
    col_name = col_name.strip().lower()
    col_name = re.sub(r"[^a-z0-9]+", "_", col_name)
    col_name = re.sub(r"_+", "_", col_name)
    return col_name.strip("_")


def standardize_all_column_names(df):
    """
    Renames ALL columns in the dataframe into snake_case format.

    Why this is important?
    - Bronze source CSV columns may contain spaces/special characters.
    - Standard naming avoids issues while querying downstream.
    - Makes Silver/Gold tables consistent across all datasets.
    """
    for c in df.columns:
        df = df.withColumnRenamed(c, standardize_column_name(c))
    return df


def trim_all_string_columns(df):
    """
    Removes unwanted leading/trailing spaces from ALL string columns.

    Why this is important?
    - SAP CSV files sometimes contain values like 'ABC ' or ' ABC'
    - Without trimming, duplicates and join mismatches happen.
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
    - These are useful in Bronze but not required in Silver dimension output.
    - Silver should contain only business columns + minimal audit columns.
    """
    meta_cols = [c for c in df.columns if c.startswith("_")]
    return df.drop(*meta_cols)


def apply_casting_rules(df, casting_rules: dict):
    """
    Applies datatype casting rules on selected columns.

    Why this is important?
    - Bronze uses inferSchema (string/int confusion may happen).
    - Silver must have stable datatypes for analytics and joins.
    - Example: year should always be int, amount should be decimal.
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
        "date": "date",
        "year": "int",
        "month": "int",
        "day": "int"
    }
}

# Columns that must not be null/blank.
# If missing -> send record to quarantine.
TABLE_REQUIRED_COLUMNS = {
    "z_material": ["material"],
    "z_prfcent": ["profit_center"],
    "z_distch": ["distribution_channel"]
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
    def build_quarantine(bronze_full_name=bronze_full_name,
                         bronze_table=bronze_table,
                         required_cols=required_cols,
                         casting_rules=casting_rules):

        # IMPORTANT:
        # Since Bronze and Silver are separate DLT pipelines,
        # we cannot use dlt.read(bronze_table).
        # We must read bronze using fully qualified name via spark.table().
        df = spark.table(bronze_full_name)

        # Step 1: Standardize column names into snake_case
        # This ensures consistency across all silver tables
        df = standardize_all_column_names(df)

        # Step 2: Trim all string columns
        # Removes extra spaces which can cause duplicates or join mismatches
        df = trim_all_string_columns(df)

        # Step 3: Apply datatype casting rules (if any)
        # Example: year -> int, date -> date, amount -> decimal
        df = apply_casting_rules(df, casting_rules)

        # Step 4: Filter out bad records (missing mandatory columns)
        bad_df = get_bad_records(df, required_cols)

        # Step 5: Add quarantine audit metadata (why/when record was quarantined)
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
    def build_silver(bronze_full_name=bronze_full_name,
                     bronze_table=bronze_table,
                     required_cols=required_cols,
                     casting_rules=casting_rules):

        # Read the bronze table (cross-pipeline read)
        df = spark.table(bronze_full_name)

        # -------------------------------
        # Core Silver Transformations
        # -------------------------------

        # Step 1: Standardize column names
        # Bronze source columns may contain spaces/special characters.
        # Silver must have clean and consistent column naming.
        df = standardize_all_column_names(df)

        # Step 2: Trim all string columns
        # Removes leading/trailing spaces so joins and comparisons work correctly.
        df = trim_all_string_columns(df)

        # Step 3: Apply datatype casting rules
        # Converts required fields into correct types (int/date/decimal).
        # Helps ensure data quality and prevents downstream schema drift issues.
        df = apply_casting_rules(df, casting_rules)

        # Step 4: Keep only good records
        # Filters out rows where mandatory business columns are missing.
        # Bad rows are stored separately in quarantine table.
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
        # This ensures stable silver output (especially useful if bronze had duplicates)
        return df.dropDuplicates()
