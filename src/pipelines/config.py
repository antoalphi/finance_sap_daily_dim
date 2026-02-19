from dataclasses import dataclass


@dataclass
class SapSDPIngestionConfig:
    """
    Configuration model used across DLT pipeline code.
    Values are passed from Databricks Asset Bundle pipeline YAML
    using the 'configuration' section.
    """
    catalog: str
    schema: str
    landing_path: str
    checkpoint_path: str
    schema_location: str
    z_tables: list


def get_config(spark, layer: str) -> SapSDPIngestionConfig:
    """
    Reads configuration values from spark.conf.
    These values are injected through pipeline YAML configuration.
     
    layer should be: 'bronze' or 'silver'
    """

    catalog = spark.conf.get("finance_SAP_dim.catalog")
 
    if layer == "bronze":
        schema = spark.conf.get("finance_SAP_dim.schema.bronze","bronze") #Default to "bronze" if not set
    elif layer == "silver":
        schema = spark.conf.get("finance_SAP_dim.schema.silver","silver") #Default to "silver" if not set
    else:
        raise ValueError(f"Invalid layer passed: {layer}")

     
    landing_path = spark.conf.get("finance_SAP_dim.adls.landing.path")
    checkpoint_path = spark.conf.get("finance_SAP_dim.checkpoint.path")
    schema_location = spark.conf.get("finance_SAP_dim.schema_path","")

    # z_tables is expected as comma-separated string: "z_date,z_distch,..."
    z_tables_str = spark.conf.get("finance_SAP_dim.z_tables", "")

    # Convert into list and remove empty/space values
    z_tables = [t.strip() for t in z_tables_str.split(",") if t.strip()]

    return SapSDPIngestionConfig(
        catalog=catalog,
        schema=schema,
        landing_path=landing_path,
        checkpoint_path=checkpoint_path,
        schema_location=schema_location,
        z_tables=z_tables
    )
