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

# PostgreSQL to BigQuery Integration

> **Direct Data Pipeline** | PostgreSQL / Neon → Google BigQuery

[![Status](https://img.shields.io/badge/Status-Production_Ready-success?style=flat-square)](.)
[![Python](https://img.shields.io/badge/Python-3.9+-blue?style=flat-square&logo=python)](https://python.org)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-13+-336791?style=flat-square&logo=postgresql)](https://postgresql.org)

---

## Overview

This module implements a **direct ETL pipeline** from PostgreSQL databases to Google BigQuery. It supports both local PostgreSQL instances and cloud-hosted Neon PostgreSQL with SSL connectivity.

### Key Features

| Feature | Description |
|---------|-------------|
| **Dual Mode** | Supports local PostgreSQL and Neon cloud PostgreSQL |
| **Incremental Load** | WRITE_APPEND strategy for continuous data accumulation |
| **Type Safety** | Automatic type casting for BigQuery compatibility |
| **SSL Support** | Secure connections for cloud databases |
| **CI/CD Ready** | GitHub Actions integration for automated scheduling |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    POSTGRESQL TO BIGQUERY PIPELINE                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   ┌────────────────────────────────────────────────────────────────────┐    │
│   │                        SOURCE LAYER                                 │    │
│   │                                                                     │    │
│   │    ┌─────────────────┐         ┌─────────────────────┐             │    │
│   │    │   PostgreSQL    │         │   Neon PostgreSQL   │             │    │
│   │    │   (localhost)   │         │   (Cloud - SSL)     │             │    │
│   │    │                 │         │                     │             │    │
│   │    │  Port: 5432     │         │  Host: *.neon.tech  │             │    │
│   │    │  DB: postgres   │         │  SSL: Required      │             │    │
│   │    └────────┬────────┘         └──────────┬──────────┘             │    │
│   │             │                             │                        │    │
│   └─────────────┼─────────────────────────────┼────────────────────────┘    │
│                 │                             │                              │
│                 ▼                             ▼                              │
│   ┌────────────────────────────────────────────────────────────────────┐    │
│   │                      EXTRACTION LAYER                               │    │
│   │                                                                     │    │
│   │    ┌─────────────────────────────────────────────────────────┐     │    │
│   │    │                    SQLAlchemy Engine                     │     │    │
│   │    │                                                          │     │    │
│   │    │   Connection String:                                     │     │    │
│   │    │   postgresql://user:pass@host:5432/database             │     │    │
│   │    │                                                          │     │    │
│   │    │   Query: SELECT * FROM public.clientes_mock              │     │    │
│   │    └─────────────────────────────────────────────────────────┘     │    │
│   │                              │                                      │    │
│   └──────────────────────────────┼──────────────────────────────────────┘    │
│                                  │                                           │
│                                  ▼                                           │
│   ┌────────────────────────────────────────────────────────────────────┐    │
│   │                    TRANSFORMATION LAYER                             │    │
│   │                                                                     │    │
│   │    ┌─────────────────────────────────────────────────────────┐     │    │
│   │    │                    Pandas DataFrame                      │     │    │
│   │    │                                                          │     │    │
│   │    │   Transformations:                                       │     │    │
│   │    │   ├── total_spend    → FLOAT64                          │     │    │
│   │    │   ├── signup_date    → DATETIME                         │     │    │
│   │    │   └── Data validation & cleansing                       │     │    │
│   │    └─────────────────────────────────────────────────────────┘     │    │
│   │                              │                                      │    │
│   └──────────────────────────────┼──────────────────────────────────────┘    │
│                                  │                                           │
│                                  ▼                                           │
│   ┌────────────────────────────────────────────────────────────────────┐    │
│   │                        LOAD LAYER                                   │    │
│   │                                                                     │    │
│   │    ┌─────────────────────────────────────────────────────────┐     │    │
│   │    │              Google BigQuery Client                      │     │    │
│   │    │                                                          │     │    │
│   │    │   Project:  postgresql-to-bq-test-485807                │     │    │
│   │    │   Dataset:  postgresql_to_bq_test_mock_data_py_generator │     │    │
│   │    │   Table:    clientes_mock                               │     │    │
│   │    │   Strategy: WRITE_APPEND                                │     │    │
│   │    └─────────────────────────────────────────────────────────┘     │    │
│   │                                                                     │    │
│   └────────────────────────────────────────────────────────────────────┘    │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Scripts Reference

### Production Script: `import_pandas_as_pd_neon.py`

Designed for **Neon PostgreSQL cloud** with environment-based configuration.

```python
# Connection Configuration
DB_HOST     = os.getenv("DB_HOST")      # Neon endpoint
DB_PASSWORD = os.getenv("DB_PASSWORD")  # Secure credential
DB_NAME     = "neondb"
DB_USER     = "neondb_owner"
DB_PORT     = "5432"
SSL_MODE    = "require"
```

| Feature | Implementation |
|---------|----------------|
| Authentication | Environment variables |
| SSL | Required (Neon mandate) |
| Error Handling | sys.exit(1) on failure |
| Logging | Print statements to stdout |

### Development Script: `import_pandas_as_pd.py`

Designed for **local PostgreSQL** development and testing.

```python
# Connection Configuration
DB_HOST     = "localhost"
DB_PORT     = "5432"
DB_NAME     = "postgres"
DB_USER     = "postgres"
DB_PASSWORD = "********"  # Local testing only
```

| Feature | Implementation |
|---------|----------------|
| Authentication | Hardcoded (dev only) |
| SSL | Not required |
| Error Handling | Exception printing |
| Logging | Print statements to stdout |

---

## Data Flow

### Source Table: `public.clientes_mock`

| Column | PostgreSQL Type | BigQuery Type | Transformation |
|--------|-----------------|---------------|----------------|
| `user_id` | VARCHAR | STRING | Direct mapping |
| `full_name` | VARCHAR | STRING | Direct mapping |
| `email` | VARCHAR | STRING | Direct mapping |
| `total_spend` | NUMERIC | FLOAT64 | `.astype(float)` |
| `signup_date` | DATE | DATETIME | `pd.to_datetime()` |
| `region` | VARCHAR | STRING | Direct mapping |
| `product_category` | VARCHAR | STRING | Direct mapping |
| `payment_method` | VARCHAR | STRING | Direct mapping |
| `is_active` | BOOLEAN | BOOLEAN | Direct mapping |
| `device_type` | VARCHAR | STRING | Direct mapping |
| `loyalty_score` | INTEGER | INT64 | Direct mapping |

### Load Strategy

```python
# BigQuery load configuration
if_exists = "append"  # Maps to WRITE_APPEND

# Behavior:
# - Table exists    → Append new rows
# - Table not exists → Create table with inferred schema
```

---

## Deployment

### Local Development

```bash
# 1. Start local PostgreSQL
# Ensure PostgreSQL is running on localhost:5432

# 2. Create source table
psql -U postgres -d postgres -c "
CREATE TABLE IF NOT EXISTS public.clientes_mock (
    user_id VARCHAR(255),
    full_name VARCHAR(255),
    email VARCHAR(255),
    total_spend NUMERIC(12,2),
    signup_date DATE,
    region VARCHAR(100),
    product_category VARCHAR(100),
    payment_method VARCHAR(100),
    is_active BOOLEAN,
    device_type VARCHAR(100),
    loyalty_score INTEGER
);
"

# 3. Load test data
# Use mock_data_generator.py to generate CSV
# Import CSV to PostgreSQL table

# 4. Run ETL
python import_pandas_as_pd.py
```

### Production (Neon + GitHub Actions)

```yaml
# .github/workflows/main.yml
name: PostgreSQL to BigQuery ETL

on:
  schedule:
    - cron: '0 12 * * *'  # Daily at 12:00 UTC
  workflow_dispatch:       # Manual trigger

env:
  DB_HOST: ${{ secrets.DB_HOST }}
  DB_PASSWORD: ${{ secrets.DB_PASSWORD }}
  GOOGLE_APPLICATION_CREDENTIALS: gcp-credentials.json

jobs:
  etl:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - name: Setup Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.9'

      - name: Install dependencies
        run: pip install -r requirements.txt

      - name: Configure GCP credentials
        run: echo '${{ secrets.GCP_CREDENTIALS }}' > gcp-credentials.json

      - name: Run ETL
        run: python import_pandas_as_pd_neon.py
```

---

## Configuration Reference

### Environment Variables

| Variable | Required | Description | Example |
|----------|----------|-------------|---------|
| `DB_HOST` | Yes (Neon) | Database hostname | `ep-xxx.us-east-2.aws.neon.tech` |
| `DB_PASSWORD` | Yes (Neon) | Database password | `••••••••` |
| `GOOGLE_APPLICATION_CREDENTIALS` | Yes | Path to GCP service account JSON | `./credentials.json` |

### BigQuery Configuration

| Parameter | Value |
|-----------|-------|
| Project ID | `postgresql-to-bq-test-485807` |
| Dataset | `postgresql_to_bq_test_mock_data_py_generator` |
| Table | `clientes_mock` |
| Region | (Inherited from dataset) |
| Write Disposition | `WRITE_APPEND` |

---

## Monitoring

### Success Indicators

```
[INFO] Connecting to PostgreSQL...
[INFO] Connection successful
[INFO] Extracting data from public.clientes_mock...
[INFO] Retrieved 1000 rows
[INFO] Applying transformations...
[INFO] Loading to BigQuery...
[INFO] Data loaded successfully to BigQuery
```

### Failure Indicators

```
[ERROR] Failed to connect to PostgreSQL: Connection refused
[ERROR] Failed to load data: 403 Access Denied
```

### BigQuery Validation Query

```sql
-- Verify data load
SELECT
    COUNT(*) as total_rows,
    MIN(signup_date) as earliest_date,
    MAX(signup_date) as latest_date,
    COUNT(DISTINCT region) as unique_regions
FROM `postgresql-to-bq-test-485807.postgresql_to_bq_test_mock_data_py_generator.clientes_mock`;
```

---

## Troubleshooting

| Issue | Cause | Resolution |
|-------|-------|------------|
| `Connection refused` | PostgreSQL not running | Start PostgreSQL service |
| `SSL required` | Neon requires SSL | Ensure `sslmode=require` in connection string |
| `403 Access Denied` | Invalid GCP credentials | Verify service account permissions |
| `Table not found` | Dataset/table missing | Create dataset in BigQuery console |
| `Type mismatch` | Schema evolution | Update transformation logic |

---

## Dependencies

```txt
pandas>=1.5.0           # Data manipulation
sqlalchemy>=2.0.0       # Database abstraction
psycopg2-binary>=2.9.0  # PostgreSQL driver
google-cloud-bigquery>=3.0.0  # BigQuery client
db-dtypes>=1.0.0        # BigQuery type support
```

---

## Related Documentation

| Resource | Link |
|----------|------|
| Parent Project | [../README.md](../README.md) |
| Neon PostgreSQL Docs | https://neon.tech/docs |
| BigQuery Python Client | https://cloud.google.com/bigquery/docs/reference/libraries |
| SQLAlchemy Docs | https://docs.sqlalchemy.org/ |

---

<div align="center">

**PostgreSQL to BigQuery Module**

</div>
