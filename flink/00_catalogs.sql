-- Iceberg REST catalog for the durable hot store.  The default catalog holds
-- Kafka sources and Postgres sinks.
CREATE CATALOG lake WITH (
  'type' = 'iceberg',
  'catalog-type' = 'rest',
  'uri' = '__ICEBERG_REST__',
  'warehouse' = 's3://warehouse/',
  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint' = 'http://minio:9000',
  's3.path-style-access' = 'true'
);

USE CATALOG lake;
CREATE DATABASE IF NOT EXISTS recon;
USE CATALOG default_catalog;

SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '30s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.externalized-checkpoint-retention' = 'RETAIN_ON_CANCELLATION';
SET 'table.exec.source.idle-timeout' = '__IDLE_TIMEOUT__';
SET 'table.exec.mini-batch.enabled' = 'false';
-- Pin the session zone: the Iceberg columns are TIMESTAMP_LTZ, and a reader in a
-- different zone must see the same instants.
SET 'table.local-time-zone' = 'UTC';
SET 'pipeline.name' = 'upi-recon-main';
