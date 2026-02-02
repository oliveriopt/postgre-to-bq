# postgre-to-bq
postgre-to-bq-test

<img width="725" height="525" alt="2026-02-02_15-52" src="https://github.com/user-attachments/assets/a46ffa23-1547-4135-a4c8-8ec7148c69fa" />

# 1. PostgreSQL

**-> Direct Transfer:**
Use **psycopg2** to connect and fetch data
Load into **pandas** DataFrame
Push directly to BigQuery using bq load or API
**_Best for: Small-to-medium datasets, simple ETLs_**

**-> Staging via GCS:**
Export to CSV/Parquet locally
Upload to GCS
Load into BigQuery from GCS
**_Best for: Large datasets, incremental loads, or when network stability is a concern_**
