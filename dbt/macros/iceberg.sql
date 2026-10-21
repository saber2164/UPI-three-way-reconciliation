{#
  DuckDB's iceberg_scan needs a concrete table location.  The standalone
  resolver (resolve_iceberg.py) asks the Iceberg REST catalog for the current
  metadata location and passes it as a dbt var:

    dbt build --vars '{"recon__match_results": "s3://warehouse/recon/match_results/metadata/....metadata.json"}'

  DuckDB 1.5 resolves the manifest list relative to the *table root*, and it
  derives the metadata file name from a version number.  The Iceberg REST
  catalog names its metadata files "<counter>-<uuid>.metadata.json", so the
  scan is pinned with the table root plus the explicit version and a matching
  version_name_format.  Pinning the version keeps a dbt run reproducible
  against one snapshot instead of silently reading whatever was committed
  mid-run.
#}
{% macro iceberg_table(namespace, table) %}
  {% set meta = var(namespace ~ "__" ~ table) %}
  {% set version = meta.split('/')[-1].replace('.metadata.json', '') %}
  {% set root = meta.rsplit('/', 2)[0] %}
  iceberg_scan('{{ root }}',
               version='{{ version }}',
               version_name_format='%s%s.metadata.json',
               allow_moved_paths=true)
{% endmacro %}

{% macro export_parquet(relation) %}
  COPY (SELECT * FROM {{ relation }}) TO 's3://warehouse/gold/{{ relation.identifier }}.parquet' (FORMAT PARQUET)
{% endmacro %}
