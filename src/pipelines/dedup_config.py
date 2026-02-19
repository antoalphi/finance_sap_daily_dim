# dedup_config.py
# Defines business key columns for each Z table to identify duplicate records.
# These keys are used in Silver layer to apply dropDuplicates() and retain only unique records per key.

DEDUP_KEYS = {
    "z_date": ["ID_DATE"],
    "z_distch": ["DISTRIBUTION_CHANNEL_CD"],
    "z_material": ["MATERIAL_ID"],
    "z_prfcent": ["PROFIT_CENTER_CD"],
    "z_prodgrp": ["PRODUCT_GROUP_HIERARCHY"],
}