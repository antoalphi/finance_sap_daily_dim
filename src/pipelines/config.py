from dataclasses import dataclass


@dataclass
class SapSDPIngestionConfig:
    catalog: str
    schema_bronze: str
    landing_path: str
    checkpoint_path: str
    schema_location: str


def get_config(spark):
    catalog = spark.conf.get("finance_SAP_dim.catalog")
    schema_bronze = spark.conf.get("finance_SAP_dim.schema.bronze")
    landing_path = spark.conf.get("finance_SAP_dim.adls.landing.path")
    checkpoint_path = spark.conf.get("finance_SAP_dim.checkpoint.path")
    schema_location = spark.conf.get("finance_SAP_dim.schema_path")

    return SapSDPIngestionConfig(
        catalog=catalog,
        schema_bronze=schema_bronze,
        landing_path=landing_path,
        checkpoint_path=checkpoint_path,
        schema_location=schema_location,
    )
