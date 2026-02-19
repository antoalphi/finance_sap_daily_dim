# dedup_config.py
# Defines business key columns for each Z table to identify duplicate records.
# These keys are used in Silver layer to apply dropDuplicates() and retain only unique records per key.
# Supports both single ID column and composite keys (multiple columns as comma-separated list in the mapping).

DEDUP_KEYS = {
    "z_date": ["ID_DATE"],
    "z_distch": ["DISTRIBUTION_CHANNEL_CD"],
    "z_material": ["MATERIAL_ID"],
    "z_prfcent": ["PROFIT_CENTER_CD"],
    "z_prodgrp": ["PRODUCT_GROUP_HIERARCHY"],
}

REQUIRED_COLUMNS = {
    "z_date": ["ID_DATE,DATE"],
    "z_distch": ["DISTRIBUTION_CHANNEL_CD,DISTRIBUTION_CHANNEL_DESC,DISTRIBUTION_CHANNEL_TYPE"],
    "z_material": ["MATERIAL_ID,MATERIAL_TYPE"],
    "z_prfcent": ["PROFIT_CENTER_CD"],
    "z_prodgrp": ["PRODUCT_GROUP_HIERARCHY"],
}