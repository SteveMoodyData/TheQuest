# The Quest - Databricks Data Engineering Pipeline

A modern data engineering solution to the Rearc Data Quest challenge, implemented using **Databricks**, **Delta Lake**, and **AWS S3** instead of traditional AWS Lambda/SQS architecture.

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Technologies Used](#technologies-used)
- [Project Structure](#project-structure)
- [Data Pipeline](#data-pipeline)
- [Setup Instructions](#setup-instructions)
- [Usage](#usage)
- [Analytics Results](#analytics-results)
- [Key Decisions](#key-decisions)
- [Future Enhancements](#future-enhancements)

---

## 🎯 Overview

This project demonstrates modern data engineering practices by building a complete ETL pipeline that:

1. **Ingests** data from multiple sources (BLS website & Data USA API)
2. **Stores** raw data in AWS S3
3. **Processes** data through Bronze → Silver → Gold layers using Delta Lake
4. **Analyzes** data to answer specific business questions
5. **Orchestrates** the entire pipeline using Databricks Workflows

### Quest Requirements Met

✅ **Part 1**: BLS data synced to S3 with automated updates  
✅ **Part 2**: Population data fetched from API and stored  
✅ **Part 3**: Data analytics with statistical calculations  
✅ **Part 4**: Automated pipeline orchestration  

**Key Differentiator**: Instead of using AWS Lambda + SQS, this solution leverages **Databricks** for a more modern, scalable, and maintainable approach.

---

## 🏗️ Architecture

### High-Level Architecture

```
┌─────────────────┐         ┌──────────────────┐
│  BLS Website    │         │  Data USA API    │
│  (HTTP Server)  │         │  (REST API)      │
└────────┬────────┘         └────────┬─────────┘
         │                           │
         │ Databricks                │ Databricks
         │ Notebook                  │ Notebook
         │ (Sync Script)             │ (API Fetch)
         ▼                           ▼
    ┌────────────────────────────────────┐
    │         AWS S3 Bucket              │
    │  - bls/pr/*.txt                    │
    │  - population/*.json               │
    └────────────┬───────────────────────┘
                 │
                 │ Auto Loader
                 │ + DLT Pipeline
                 ▼
    ┌─────────────────────────────────────┐
    │   Databricks Unity Catalog          │
    │                                      │
    │  ┌──────────────────────────────┐   │
    │  │  Bronze Layer (Raw Data)     │   │
    │  │  - bronze_bls_pr_data        │   │
    │  │  - bronze_population         │   │
    │  └──────────┬───────────────────┘   │
    │             │                        │
    │  ┌──────────▼───────────────────┐   │
    │  │  Silver Layer (Cleaned)      │   │
    │  │  - silver_bls_pr_data        │   │
    │  │  - silver_population         │   │
    │  └──────────┬───────────────────┘   │
    │             │                        │
    │  ┌──────────▼───────────────────┐   │
    │  │  Gold Layer (Analytics)      │   │
    │  │  - gold_population_stats     │   │
    │  │  - gold_best_year            │   │
    │  │  - gold_series_filtered      │   │
    │  └──────────────────────────────┘   │
    └─────────────────────────────────────┘
```

### Medallion Architecture Flow

**Bronze Layer** (Raw) → **Silver Layer** (Cleaned) → **Gold Layer** (Analytics)

---

## 🛠️ Technologies Used

| Technology | Purpose |
|-----------|---------|
| **Databricks** | Unified analytics platform for ETL and orchestration |
| **Delta Lake** | ACID transactions, time travel, schema enforcement |
| **Delta Live Tables (DLT)** | Declarative pipeline framework |
| **Auto Loader** | Incremental file ingestion from S3 |
| **Unity Catalog** | Data governance and lineage tracking |
| **AWS S3** | Data lake storage |
| **Python** | Scripting and data processing |
| **PySpark** | Distributed data processing |

---

## 📁 Project Structure

```
TheQuest/
├── BLS_to_S3_loader_DBX.py          # Syncs BLS website files to S3
├── BLS_autoloader.py                # Auto Loader: S3 → Bronze table
├── api_load_file.py                 # Fetches population API → Volume
├── thequest_ETL_pipeline/
│   └── transformations/
│       └── dlt_pipeline.py          # DLT pipeline: Bronze → Silver → Gold
├── thequest_data_analytics.ipynb    # Analytics queries and results
├── The Quest Full Job.yml           # Databricks Workflow configuration
├── images/                          # Architecture diagrams
└── README.md                        # This file
```

---

## 🔄 Data Pipeline

### 1️⃣ **Data Ingestion**

#### BLS Data (Bureau of Labor Statistics)

**Source**: https://download.bls.gov/pub/time.series/pr/

**Process**: 
- Python script scrapes directory listing using regex
- Downloads all files to S3 (overwrites existing)
- Deletes orphaned files (keeps S3 in sync with source)
- Respects BLS robots policy with custom User-Agent header

**Key Features**:
```python
# Full sync approach - simple and effective
website_files = get_file_list_from_bls()
s3_files = list_s3_objects()
orphans = s3_files - website_files
delete_orphaned_files(orphans)
```

**Files Ingested**:
- `pr.data.0.Current` - Current productivity data
- `pr.data.1.AllData` - Historical productivity data
- `pr.series` - Series definitions
- `pr.contacts`, `pr.footnote`, `pr.measure`, etc.

#### Population Data (Data USA API)

**Source**: https://datausa.io API endpoint

**Endpoint**:
```
https://honolulu-api.datausa.io/tesseract/data.jsonrecords
?cube=acs_yg_total_population_1
&drilldowns=Year%2CNation
&locale=en
&measures=Population
```

**Process**:
- REST API call fetches US population by year (2013-2023)
- Stores JSON in Unity Catalog Volume
- Timestamped files enable historical tracking
- Volume path: `/Volumes/thequest/bronze/datausa_io/`

### 2️⃣ **Bronze Layer** (Raw Data)

**Purpose**: Store data exactly as received from source

| Table | Source | Records | Schema Approach |
|-------|--------|---------|-----------------|
| `bronze_bls_pr_data_current` | S3 via Auto Loader | ~20,000 | Inferred with column trimming |
| `bronze_population` | Volume JSON file | 10 | Column mapping enabled |

**Key Features**:
- **Auto Loader** for incremental file processing
- **Schema evolution** handling for changing data structures
- **Metadata tracking**: `_source_file`, `_ingestion_time`
- **Column mapping** enabled to handle special characters in JSON

**Bronze BLS Schema**:
```sql
series_id       STRING    -- Series identifier (e.g., PRS30006011)
year            STRING    -- Year (stored as string in bronze)
period          STRING    -- Quarter (Q01, Q02, Q03, Q04, Q05)
value           STRING    -- Productivity value (stored as string)
footnote_codes  STRING    -- Data footnotes
_source_file    STRING    -- Source filename
_ingestion_time TIMESTAMP -- When data was loaded
```

**Bronze Population Schema**:
```sql
data            STRUCT    -- Nested JSON structure
  - data        ARRAY     -- Array of population records
    - Nation ID STRING
    - Nation    STRING
    - Year      STRING
    - Population STRING
metadata        STRUCT    -- API metadata
_source_file    STRING
_ingestion_timestamp TIMESTAMP
```

### 3️⃣ **Silver Layer** (Cleaned & Typed)

**Purpose**: Business-ready data with proper types and cleaned columns

**Transformations Applied**:
- ✅ Trim all string columns (remove leading/trailing whitespace)
- ✅ Cast to appropriate data types
- ✅ Filter invalid records with data quality rules
- ✅ Standardize column names (remove special characters)
- ✅ Flatten nested JSON structures

**Silver BLS Schema**:
```sql
series_id       STRING    -- Trimmed series identifier
year            SMALLINT  -- Converted to integer
period          STRING    -- Trimmed quarter code
value           DOUBLE    -- Converted to numeric (decimal values)
footnote_codes  STRING    -- Trimmed footnotes
_source_file    STRING    
_ingestion_time TIMESTAMP
```

**Silver Population Schema**:
```sql
nation_id       STRING    -- Trimmed (e.g., "01000US")
nation          STRING    -- Trimmed (e.g., "United States")
year            INT       -- Converted to integer
population      BIGINT    -- Converted to long integer
_source_file    STRING
_ingestion_timestamp TIMESTAMP
```

**Data Quality Rules**:
```python
@dlt.expect_or_drop("valid_year", "year IS NOT NULL")
@dlt.expect_or_drop("valid_population", "population > 0")
@dlt.expect_or_drop("valid_value", "value IS NOT NULL")
```

### 4️⃣ **Gold Layer** (Analytics)

**Purpose**: Pre-aggregated tables optimized for analytics and reporting

| Table | Business Question | Records |
|-------|-------------------|---------|
| `gold_population_mean_stddev` | What's the population trend 2013-2018? | 1 row |
| `gold_best_year` | Which year had highest productivity per series? | ~100 rows |
| `gold_population_series_filtered` | How does series PRS30006032 correlate with population? | ~10 rows |

**Gold Population Mean/StdDev**:
```sql
mean_population     DOUBLE    -- 321,403,141.17
stddev_population   DOUBLE    -- 4,158,869.17
start_year          INT       -- 2013
end_year            INT       -- 2018
year_count          INT       -- 6
```

**Gold Best Year**:
```sql
series_id   STRING   -- BLS series identifier
year        SMALLINT -- Year with highest productivity
value       DOUBLE   -- Total summed value for all quarters
```

**Gold Population Series Filtered**:
```sql
series_id   STRING   -- PRS30006032
year        SMALLINT -- Year
period      STRING   -- Q01
value       DOUBLE   -- Productivity value
population  BIGINT   -- US population for that year
```

---

## 🚀 Setup Instructions

### Prerequisites

- Databricks Workspace (AWS)
- AWS Account with S3 access
- Python 3.8+ (for local development/testing)
- Git

### 1. Clone Repository

```bash
git clone https://github.com/SteveMoodyData/TheQuest.git
cd TheQuest
```

### 2. Configure Databricks Secrets

```bash
# Create secret scope for AWS credentials
databricks secrets create-scope aws

# Add AWS credentials (will prompt for values)
databricks secrets put-secret aws access_key_id
databricks secrets put-secret aws secret_access_key

# Verify secrets were created
databricks secrets list-secrets aws
```

**Alternative**: Use Databricks UI to create secrets in Settings → Developer → Secrets

### 3. Create S3 Bucket

```bash
# Create S3 bucket
aws s3 mb s3://your-bucket-name

# Update bucket name in notebooks
# Edit BLS_to_S3_loader_DBX.py: S3_BUCKET = 'your-bucket-name'
```

### 4. Create Unity Catalog Structure

Run in Databricks SQL or notebook:

```sql
-- Create catalog
CREATE CATALOG IF NOT EXISTS thequest;

-- Create schemas
CREATE SCHEMA IF NOT EXISTS thequest.main;
CREATE SCHEMA IF NOT EXISTS thequest.bronze;

-- Create volume for API data
CREATE VOLUME IF NOT EXISTS thequest.bronze.datausa_io;
```

### 5. Import Notebooks to Databricks

**Via Databricks UI**:
1. Go to Workspace → Import
2. Upload each Python notebook:
   - `BLS_to_S3_loader_DBX.py`
   - `BLS_autoloader.py`
   - `api_load_file.py`
   - `thequest_ETL_pipeline/transformations/dlt_pipeline.py`

**Via Databricks CLI**:
```bash
databricks workspace import BLS_to_S3_loader_DBX.py /Users/your-email/TheQuest/
databricks workspace import BLS_autoloader.py /Users/your-email/TheQuest/
databricks workspace import api_load_file.py /Users/your-email/TheQuest/
```

### 6. Create Delta Live Tables Pipeline

**Via UI**:
1. Navigate to Workflows → Delta Live Tables
2. Click "Create Pipeline"
3. Configure:
   - **Name**: `TheQuest_ETL_Pipeline`
   - **Notebook**: Select `thequest_ETL_pipeline/transformations/dlt_pipeline.py`
   - **Target**: `main`
   - **Catalog**: `thequest`
   - **Storage Location**: (optional)
   - **Cluster Mode**: Development (for testing)
4. Click "Create"

**Via CLI**:
```bash
# Create pipeline configuration JSON and deploy
databricks pipelines create --settings pipeline_config.json
```

### 7. Create Databricks Workflow

**Option A: Import YAML**
1. Go to Workflows → Jobs
2. Click "Create Job" → "Import from YAML"
3. Upload `The Quest Full Job.yml`

**Option B: Manual Creation**
1. Create new Job: "The Quest - Full Pipeline"
2. Add tasks in order:
   - Task 1: Run `BLS_to_S3_loader_DBX.py`
   - Task 2: Run `api_load_file.py`
   - Task 3: Run `BLS_autoloader.py` (depends on Task 1)
   - Task 4: Start DLT Pipeline (depends on Tasks 2 & 3)
3. Configure schedule: Daily at 2:00 AM PST
4. Add email notifications for failures

---

## 📊 Usage

### Manual Execution (Development)

Run notebooks in this order:

```python
# Step 1: Sync BLS data from website to S3
%run ./BLS_to_S3_loader_DBX

# Step 2: Fetch population data from API to Volume
%run ./api_load_file

# Step 3: Load BLS data from S3 to Bronze table
%run ./BLS_autoloader

# Step 4: Run DLT pipeline (Bronze → Silver → Gold)
# Navigate to Delta Live Tables and click "Start"
```

### Automated Workflow (Production)

The Databricks Workflow orchestrates the entire pipeline:

**Schedule**: Daily at 2:00 AM PST

**Execution Flow**:
1. BLS sync runs (~2 minutes)
2. API fetch runs (~30 seconds)
3. Auto Loader processes S3 files (~1 minute)
4. DLT pipeline transforms data (~2 minutes)
5. Email notification sent on completion/failure

**Monitoring**:
- View job runs in Workflows UI
- Check DLT pipeline lineage graph
- Review logs for each task
- Query audit tables for data quality metrics

### Ad-Hoc Analytics

Query the gold tables directly:

```sql
-- Population statistics
SELECT * FROM thequest.main.gold_population_mean_stddev;

-- Best performing years by series
SELECT * FROM thequest.main.gold_best_year
ORDER BY value DESC
LIMIT 10;

-- Series with population correlation
SELECT * FROM thequest.main.gold_population_series_filtered
ORDER BY year DESC;
```

---

## 📈 Analytics Results

### Question 1: Population Statistics (2013-2018)

**Requirement**: Calculate mean and standard deviation of US population for years 2013-2018 inclusive.

**SQL Query**:
```sql
SELECT 
  AVG(population) as mean_population,
  STDDEV(population) as stddev_population,
  MIN(year) as start_year,
  MAX(year) as end_year,
  COUNT(*) as year_count
FROM thequest.main.silver_population
WHERE year BETWEEN 2013 AND 2018
```

**Results**:
| Metric | Value |
|--------|-------|
| **Mean Population** | 321,403,141 |
| **Standard Deviation** | 4,158,869 |
| **Years Analyzed** | 2013-2018 (6 years) |

**Interpretation**: The US population grew steadily during this period with relatively low variance, indicating stable demographic trends.

### Question 2: Best Year by Series

**Requirement**: For every series_id, find the year with the maximum sum of quarterly values.

**SQL Query**:
```sql
WITH yearly_sums AS (
  SELECT 
    series_id,
    year,
    SUM(value) as total_value
  FROM thequest.main.silver_bls_pr_data_current
  WHERE period LIKE 'Q%'  -- Only quarterly data
  GROUP BY series_id, year
),
ranked AS (
  SELECT 
    series_id,
    year,
    total_value,
    ROW_NUMBER() OVER (PARTITION BY series_id ORDER BY total_value DESC) as rank
  FROM yearly_sums
)
SELECT series_id, year, total_value as value
FROM ranked
WHERE rank = 1
ORDER BY series_id
```

**Sample Results**:

| series_id | year | value |
|-----------|------|-------|
| PRS30006011 | 1996 | 7.0 |
| PRS30006012 | 2000 | 8.0 |
| PRS30006032 | 2021 | 5.8 |
| PRS30006042 | 1997 | 12.3 |

**Interpretation**: Different productivity series peaked in different years, reflecting varied economic conditions across sectors.

### Question 3: BLS Series with Population

**Requirement**: Generate a report showing series PRS30006032, period Q01, with corresponding population data.

**SQL Query**:
```sql
SELECT 
  b.series_id,
  b.year,
  b.period,
  b.value,
  p.population
FROM thequest.main.silver_bls_pr_data_current b
LEFT JOIN thequest.main.silver_population p 
  ON b.year = p.year
WHERE b.series_id = 'PRS30006032' 
  AND b.period = 'Q01'
ORDER BY b.year DESC
```

**Sample Output**:

| series_id | year | period | value | population |
|-----------|------|--------|-------|------------|
| PRS30006032 | 2023 | Q01 | 1.2 | 334914896 |
| PRS30006032 | 2022 | Q01 | 0.8 | 333287562 |
| PRS30006032 | 2021 | Q01 | 2.1 | 331893745 |
| PRS30006032 | 2019 | Q01 | 1.5 | 328239523 |
| PRS30006032 | 2018 | Q01 | 1.9 | 327167439 |

**Interpretation**: This combined view allows analysis of productivity trends relative to population growth, useful for per-capita productivity calculations.

### Data Quality Metrics

From DLT expectations:

| Check | Records Passed | Records Dropped |
|-------|----------------|-----------------|
| `valid_year` (Population) | 10 | 0 |
| `valid_population` (Population) | 10 | 0 |
| `valid_year` (BLS) | 20,847 | 0 |
| `valid_value` (BLS) | 20,847 | 0 |

**Data Quality**: 100% pass rate on all quality checks

---

## 🎯 Key Decisions

### Why Databricks over AWS Lambda?

| Aspect | AWS Lambda + SQS | Databricks Solution |
|--------|------------------|---------------------|
| **Development Speed** | Complex event-driven orchestration | Declarative pipelines with DLT |
| **Data Quality** | Manual validation code | Built-in expectations/assertions |
| **Monitoring** | CloudWatch logs (scattered) | Rich UI with data lineage |
| **Schema Evolution** | Manual schema updates | Automatic with Delta Lake |
| **Scalability** | 15-min timeout limits | Auto-scaling clusters |
| **Debugging** | Log diving | Interactive notebooks |
| **Cost** | Per-invocation (unpredictable) | Per-DBU (more predictable) |
| **Testing** | Requires mocking | Easy local testing |

### Why Delta Live Tables?

**Advantages**:
- **Declarative syntax**: Define WHAT you want, not HOW to get it
- **Automatic dependencies**: DLT figures out optimal execution order
- **Data quality**: Built-in expectations for validation
- **Lineage tracking**: Visual data flow graphs
- **Incremental processing**: Efficient change detection
- **Error handling**: Automatic retries and recovery

**Example**:
```python
# Traditional approach (imperative)
df = spark.read.table("bronze")
df_clean = df.filter(col("year").isNotNull())
df_clean.write.mode("overwrite").saveAsTable("silver")

# DLT approach (declarative)
@dlt.table
@dlt.expect_or_drop("valid_year", "year IS NOT NULL")
def silver():
  return dlt.read("bronze")
```

### Why Medallion Architecture?

**Bronze → Silver → Gold Pattern Benefits**:

1. **Separation of Concerns**
   - Bronze: Store everything (audit trail)
   - Silver: Clean for consumption
   - Gold: Optimize for analytics

2. **Reusability**
   - Multiple gold tables can reference same silver table
   - Changes to analytics don't affect raw data

3. **Auditability**
   - Can always trace back to original source
   - Time travel to any historical state

4. **Performance**
   - Gold tables pre-compute aggregations
   - Queries run in milliseconds vs. minutes

5. **Data Quality**
   - Validation at each layer
   - Bad data contained in bronze

### Technology Trade-offs

| Decision | Alternative Considered | Why This Choice |
|----------|------------------------|-----------------|
| **Databricks** | AWS Glue | Better notebook experience, DLT |
| **Delta Lake** | Parquet files | ACID transactions, time travel |
| **Python** | Scala/Java | Faster development, readable |
| **Unity Catalog** | Hive Metastore | Better governance, lineage |
| **S3** | DBFS | Standard, portable, cheaper |

---

## 🔮 Future Enhancements

### Short-term (Next Sprint)

- [ ] **CI/CD Pipeline**: GitHub Actions for automated testing and deployment
- [ ] **Data Quality Dashboard**: Databricks SQL dashboard showing data quality metrics
- [ ] **Alerting**: PagerDuty integration for critical failures
- [ ] **Cost Monitoring**: Track DBU usage and optimize cluster sizing

### Medium-term (Next Quarter)

- [ ] **Additional Data Sources**
  - Census Bureau demographic data
  - Bureau of Economic Analysis (BEA) GDP data
  - Federal Reserve economic indicators
  
- [ ] **Advanced Analytics**
  - Time-series forecasting for population trends
  - Anomaly detection for productivity outliers
  - Correlation analysis between economic indicators

- [ ] **Performance Optimization**
  - Z-ordering on common query patterns
  - Liquid clustering for better file layout
  - Photon acceleration for compute-intensive queries

### Long-term (Next Year)

- [ ] **Machine Learning Pipeline**
  - MLflow integration for experiment tracking
  - Feature store for reusable features
  - Automated model retraining on new data
  - Productivity prediction models

- [ ] **Real-time Streaming**
  - Replace batch ingestion with Kafka/Kinesis
  - Structured Streaming for real-time updates
  - Low-latency dashboards (< 1 minute lag)

- [ ] **Data Mesh Architecture**
  - Domain-oriented data ownership
  - Self-serve data platform
  - Federated governance model

- [ ] **Advanced Governance**
  - Row/column-level security
  - Data masking for PII
  - Automated data cataloging
  - Compliance reporting (GDPR, SOC2)

---

## 🐛 Known Issues

1. **Schema Evolution**: BLS occasionally adds new columns. Current workaround: `mergeSchema=true` in Auto Loader
2. **API Rate Limiting**: Data USA API has rate limits. Consider caching responses
3. **File Encoding**: Some BLS files may have non-UTF-8 characters. Handled by specifying encoding in read operations

---

## 📚 References

### Documentation

- [Databricks Delta Live Tables](https://docs.databricks.com/delta-live-tables/)
- [Auto Loader](https://docs.databricks.com/ingestion/auto-loader/)
- [Delta Lake](https://delta.io/)
- [Unity Catalog](https://docs.databricks.com/data-governance/unity-catalog/)

### Data Sources

- [BLS Productivity Data](https://www.bls.gov/lpc/)
- [Data USA API Documentation](https://datausa.io/about/api/)

### Best Practices

- [Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)
- [Data Engineering Best Practices](https://docs.databricks.com/lakehouse/data-engineering.html)

---

## 📝 License

This project is created for educational purposes as part of the Rearc Data Quest challenge.

---

## 👤 Author

**Steve Moody**  
Lead Data Engineer | Databricks Certified Data Engineer Associate  
Portland, Oregon

- GitHub: [@SteveMoodyData](https://github.com/SteveMoodyData)
- LinkedIn: [Steve Moody](https://www.linkedin.com/in/your-profile)
- Email: steve.moody@gmail.com

### Skills Demonstrated

- ✅ Data Pipeline Design (Medallion Architecture)
- ✅ Databricks Platform Engineering
- ✅ Delta Lake & Delta Live Tables
- ✅ PySpark & SQL Optimization
- ✅ Data Quality & Testing
- ✅ Cloud Infrastructure (AWS S3)
- ✅ Data Governance (Unity Catalog)
- ✅ ETL/ELT Best Practices

---

## 🙏 Acknowledgments

- **[Bureau of Labor Statistics](https://www.bls.gov/)** for providing high-quality, publicly available economic data
- **[Data USA](https://datausa.io/)** for the excellent population data API
- **Databricks Community** for outstanding documentation and support
- **Delta Lake OSS Community** for building amazing open-source technology



## 📊 Project Statistics

- **Lines of Code**: ~2,000
- **Number of Notebooks**: 4
- **Data Tables Created**: 9 (3 Bronze, 3 Silver, 3 Gold)
- **Data Volume**: ~2 GB in S3
- **Pipeline Execution Time**: ~6 minutes end-to-end
- **Development Time**: 2 weeks
- **Technologies Used**: 8 major tools/platforms
