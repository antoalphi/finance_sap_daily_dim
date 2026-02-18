import dlt
import re

from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    current_timestamp,
    current_date,
    lit,
    sha2,
    concat_ws,
    col
)

from config import get_config


def clean_table_name(folder_name: str) -> str:
    name = re.sub(r"[^a-zA-Z0-9_]", "_", folder_name)
    name = re.sub(r"_+", "_", name)
    return name.lower()


def list_csv_files(folder_path: str):
    files = dbutils.fs.ls(folder_path)
    return [f.path for f in files if f.path.lower().endswith(".csv")]


def detect_delimiter(file_path: str) -> str:
    """
    Detect delimiter from first line (header).
    """
    sample = dbutils.fs.head(file_path, 4096) #This will display the first 4096 bytes of the file. DEFAULT is 1024, which may not be enough for larger header lines.

    candidates = [",", ";", "|", "\t"]
    best_delim = ","
    best_count = 0

    for d in candidates:
        count = sample.count(d)
        if count > best_count:
            best_count = count
            best_delim = d

    return best_delim


cfg = get_config(spark)
pipeline_run_id = spark.conf.get("spark.databricks.job.runId", "unknown")

# Loop only configured Z tables
for folder_name in cfg.z_tables:

    folder_name = folder_name.strip()
    table_name = clean_table_name(folder_name)

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
    def load_table(folder_path=folder_path, folder_name=folder_name):

        csv_files = list_csv_files(folder_path)

        if not csv_files:
            # Return empty dataframe (avoids schema inference failure)
            return spark.createDataFrame([], schema="dummy STRING").limit(0)

        dfs = []

        for file_path in csv_files:
            delim = detect_delimiter(file_path)

            df = (
                spark.read.format("csv")
                .option("header", "true")
                .option("sep", delim)
                .option("inferSchema", "true")
                .option("includeMetadata", "true")
                .load(file_path)
            )

            df = (
                df.withColumn("_ingestion_time", current_timestamp())
                  .withColumn("_load_date", current_date())
                  .withColumn("_pipeline_run_id", lit(pipeline_run_id))
                  .withColumn("_source_file", col("_metadata.file_path"))
                  .withColumn("_source_folder", lit(folder_name))
                  .withColumn("_delimiter_used", lit(delim))
            )

            dfs.append(df)

        # Union all files into one dataframe
        final_df = reduce(DataFrame.unionByName, dfs)

        # Record hash (computed after union)
        record_hash = sha2(concat_ws("||", *final_df.columns), 256)

        return final_df.withColumn("_record_hash", record_hash)
