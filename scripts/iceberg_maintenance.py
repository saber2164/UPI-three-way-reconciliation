"""Iceberg table maintenance: bin-pack compaction, snapshot expiry (N days), orphan removal. Generic — reused by Project 1 too."""
import sys
from datetime import datetime, timedelta, timezone
from pyspark.sql import SparkSession
table, days = sys.argv[1], int(sys.argv[2])
spark = SparkSession.builder.appName(f"maintenance-{table}").getOrCreate()
cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
print(spark.sql(f"CALL lake.system.rewrite_data_files(table => '{table}', strategy => 'binpack', options => map('target-file-size-bytes','134217728','min-input-files','5'))").collect())
print(spark.sql(f"CALL lake.system.rewrite_manifests(table => '{table}')").collect())
print(spark.sql(f"CALL lake.system.expire_snapshots(table => '{table}', older_than => TIMESTAMP '{cutoff}', retain_last => 3)").collect())
print(spark.sql(f"CALL lake.system.remove_orphan_files(table => '{table}', older_than => TIMESTAMP '{cutoff}')").collect())
