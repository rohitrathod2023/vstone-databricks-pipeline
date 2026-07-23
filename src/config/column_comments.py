"""
Column-level descriptions for Bronze/Silver tables -- Unity Catalog
discoverability, same motivation as `utils.metadata.apply_table_comments`'s
table-level comments, one level down. Keyed the same way as
`config/schemas.py`/`config/silver_schemas.py` so `config_loader.py` can
resolve them with the identical lookup pattern (including Bronze's
raw_streets/bronze_streets source_key indirection).

Gold's column comments live inline in `pipelines/gold/table_schemas.py`
instead of here -- Gold's DDL already needs its own hand-written column list
(renamed/derived columns, surrogate keys) that doesn't map 1:1 onto these
raw source columns, so there's nothing to share.
"""
from __future__ import annotations

# Every Bronze/Silver table carries these 4 audit columns (see utils.audit) --
# defined once and merged into each table's dict below rather than repeated.
_AUDIT_COLUMN_COMMENTS = {
    "load_dt": "Timestamp this row was loaded into this table.",
    "source_format": "File format of the source this row was loaded from (csv/json/xml).",
    "source_file": "Name of the specific source file this row was loaded from.",
    "run_id": "Identifier for the pipeline run that loaded this row.",
}

_REJECTION_REASON_COMMENT = {
    "rejection_reason": (
        "Which quarantine rule this row failed -- see the paired non-rejected "
        "table's build function for the rule definitions."
    ),
}

# ---------------------------------------------------------------------------
# Bronze -- keyed the same as config/schemas.py's SCHEMAS dict (raw_* base
# keys; chunk1-4/bronze_* entries resolve to these via sources.yml's own
# source_key indirection, same as get_source_schema()).
# ---------------------------------------------------------------------------

_CARS_COLUMNS = {
    "enter": "Vehicles entering the intersection in this reading, observed range 0-35.",
    "exit": "Vehicles exiting the intersection in this reading, observed range 0-35.",
    "date": "Reading timestamp, ISO8601.",
    "id": (
        "Traffic reading sequence number, observed range 0-998 -- NOT a unique row "
        "identifier alone; id+location+date together are the confirmed unique/dedup key."
    ),
    "location": "Intersection identifier, observed range 1-14. FK to node_locations.location (Dim_Location in Gold).",
}

_STREETS_COLUMNS = {
    "noise": "Noise level reading, observed range 0-~46.9.",
    "pollution": "Pollution level reading, observed range 0-~46.9.",
    "date": "Reading timestamp, ISO8601.",
    "light": "Light level reading, observed range 5.14E-8-83.7.",
    "raining": (
        "Rain intensity reading. Real found anomaly: observed range is -0.9999999834 to "
        "100.9999, outside the physically-valid 0-100 range -- quarantined at Silver "
        "(raining < 0 OR raining > 100)."
    ),
    "street_id": "Street identifier, observed range 1-36. FK to streets_list.street_id (Dim_Street in Gold).",
}

_NODE_LOCATIONS_COLUMNS = {
    "latitude": "Intersection latitude, 6-decimal precision.",
    "longitude": "Intersection longitude, 6-decimal precision.",
    "location": (
        "Intersection identifier, 1-14, unique per row. location=7 has bad (0,0) "
        "coordinates -- quarantined at Silver."
    ),
}

_STREETS_LIST_COLUMNS = {
    "street": "Street name.",
    "long": "Street length in meters -- NOT longitude, despite the name.",
    "latitude": "Street latitude, 6-decimal precision.",
    "longitude": "Street longitude, 6-decimal precision.",
    "dangerous": "Danger rating for this street, observed range 0.1-0.9 (higher = more dangerous).",
    "street_id": "Street identifier, 1-36, unique per row.",
}

_TELEGRAM_COLUMNS = {
    "message": (
        "Free-text citizen report. Some raw values have leading/trailing whitespace "
        "(real found data-quality issue)."
    ),
    "date": (
        "Report date as a raw string in DD/MM/YYYY format (confirmed via hard evidence, "
        "e.g. 30/07/2023 -- no month can be 30) -- not yet parsed to a real date at Bronze."
    ),
    "hour": "Report time-of-day as a raw HH:mm:ss string -- NOT a real timestamp, has no date component at Bronze.",
}

_CARS_TABLE_COMMENTS = {**_CARS_COLUMNS, **_AUDIT_COLUMN_COMMENTS}

COLUMN_COMMENTS = {
    # chunk1-4 have no source_key indirection in sources.yml (unlike
    # bronze_streets etc.) -- same reason schemas.py's SCHEMAS dict lists
    # them explicitly instead of relying on lookup_key resolution.
    "raw_cars": _CARS_TABLE_COMMENTS,
    "chunk1_csv": _CARS_TABLE_COMMENTS,
    "chunk2_csv": _CARS_TABLE_COMMENTS,
    "chunk3_json": _CARS_TABLE_COMMENTS,
    "chunk4_xml": _CARS_TABLE_COMMENTS,
    "raw_streets": {**_STREETS_COLUMNS, **_AUDIT_COLUMN_COMMENTS},
    "raw_node_locations": {**_NODE_LOCATIONS_COLUMNS, **_AUDIT_COLUMN_COMMENTS},
    "raw_streets_list": {**_STREETS_LIST_COLUMNS, **_AUDIT_COLUMN_COMMENTS},
    "raw_telegram": {**_TELEGRAM_COLUMNS, **_AUDIT_COLUMN_COMMENTS},
}

# ---------------------------------------------------------------------------
# Silver -- keyed the same as config/silver_schemas.py's SILVER_SCHEMAS dict.
# _rejected variants are the parent table's columns + rejection_reason.
# ---------------------------------------------------------------------------

_SILVER_LOCATIONS_COLUMNS = {
    "location": "Intersection identifier, 1-14.",
    "latitude": "Intersection latitude, 6-decimal precision.",
    "longitude": "Intersection longitude, 6-decimal precision.",
}

_SILVER_STREETS_COLUMNS = {
    "street": "Street name.",
    "long": "Street length in meters -- NOT longitude, despite the name.",
    "latitude": "Street latitude, 6-decimal precision.",
    "longitude": "Street longitude, 6-decimal precision.",
    "dangerous": "Danger rating for this street, valid range 0.1-0.9 (higher = more dangerous).",
    "street_id": "Street identifier, 1-36, unique per row.",
}

_SILVER_TRAFFIC_COLUMNS = {
    "id": (
        "Traffic reading sequence number, 0-998 -- NOT unique alone; id+location+date "
        "together are the dedup key applied at this table."
    ),
    "location": "Intersection identifier, 1-14.",
    "enter": "Vehicles entering the intersection in this reading.",
    "exit": "Vehicles exiting the intersection in this reading.",
    "date": "Reading timestamp.",
    "source_technique": (
        "Which of the 4 Bronze ingestion techniques (COPY INTO/DLT/Auto Loader/PySpark) "
        "this row came from -- not brief-required, added for observability."
    ),
}

_SILVER_ENVIRONMENT_COLUMNS = {
    "street_id": "Street identifier, 1-36.",
    "date": "Reading timestamp.",
    "noise": "Noise level reading.",
    "pollution": "Pollution level reading.",
    "light": "Light level reading.",
    "raining": (
        "Rain intensity reading, valid range enforced to 0-100 -- rows outside this range "
        "are quarantined to silver_environment_rejected."
    ),
}

_SILVER_TELEGRAM_COLUMNS = {
    "message": "Free-text citizen report, whitespace-normalized from the raw Bronze value.",
    "event_timestamp": (
        "Real timestamp built by combining the raw date (DD/MM/YYYY) + hour (HH:mm:ss) "
        "strings -- the two originals are dropped once combined."
    ),
}

SILVER_COLUMN_COMMENTS = {
    "silver_locations": {**_SILVER_LOCATIONS_COLUMNS, **_AUDIT_COLUMN_COMMENTS},
    "silver_locations_rejected": {**_SILVER_LOCATIONS_COLUMNS, **_AUDIT_COLUMN_COMMENTS, **_REJECTION_REASON_COMMENT},
    "silver_streets": {**_SILVER_STREETS_COLUMNS, **_AUDIT_COLUMN_COMMENTS},
    "silver_streets_rejected": {**_SILVER_STREETS_COLUMNS, **_AUDIT_COLUMN_COMMENTS, **_REJECTION_REASON_COMMENT},
    "silver_traffic": {**_SILVER_TRAFFIC_COLUMNS, **_AUDIT_COLUMN_COMMENTS},
    "silver_traffic_rejected": {**_SILVER_TRAFFIC_COLUMNS, **_AUDIT_COLUMN_COMMENTS, **_REJECTION_REASON_COMMENT},
    "silver_environment": {**_SILVER_ENVIRONMENT_COLUMNS, **_AUDIT_COLUMN_COMMENTS},
    "silver_environment_rejected": {
        **_SILVER_ENVIRONMENT_COLUMNS,
        **_AUDIT_COLUMN_COMMENTS,
        **_REJECTION_REASON_COMMENT,
    },
    "silver_telegram": {**_SILVER_TELEGRAM_COLUMNS, **_AUDIT_COLUMN_COMMENTS},
    "silver_telegram_rejected": {**_SILVER_TELEGRAM_COLUMNS, **_AUDIT_COLUMN_COMMENTS, **_REJECTION_REASON_COMMENT},
}
