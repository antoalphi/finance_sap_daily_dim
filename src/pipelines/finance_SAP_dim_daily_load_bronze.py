import dlt
import re
from pyspark.sql.functions import current_timestamp, col

from config import get_config


def clean_table_name(file_name: str) -> str:
    # remove extension
    name = file_name.replace(".csv", "")

    # replace special characters with underscore
    name = re.sub(r"[^a-zA-Z0-9_]", "_", name)

    # avoid multiple underscores
    name = re.sub(r"_+", "_", name)

    return name.lower()


def get_file_list(path: str):
    files = dbutils.fs.ls(path)
    return [f.path for f in files if f.path.endswith(".csv")]


cfg = get_config(spark)

landing_files = get_file_list(cfg.landing_path)

for file_path in landing_files:

    file_name = file_path.split("/")[-1]
    table_name = clean_table_name(file_name)

    @dlt.table(
        name=table_name,
        comment=f"Bronze dimension table created from landing file {file_name}",
        table_properties={
            "quality": "bronze",
            "source": "finance SAP dimension data",
            "project": "Finance SAP dim daily load",
        },
    )
    def load_table(file_path=file_path):
        df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(file_path)

        return df.withColumn("_ingestion_time", current_timestamp()).withColumn("_source_file", col("_metadata.file_path"))
