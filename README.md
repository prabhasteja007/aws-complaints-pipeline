# Consumer Complaints Analytics Pipeline

An end-to-end cloud data pipeline ingesting 500K CFPB consumer complaint records through AWS (S3 → Glue → Athena) with a Power BI dashboard surfacing product-level, company-level, and geographic insights.

---

## Architecture

```
CFPB Open Data (8.3GB CSV)
        │
        ▼
  AWS CloudShell          ← download + convert to Parquet within AWS network
        │
        ▼
  Amazon S3               ← complaints-pipeline-teja-ohio / raw/
  (Parquet, Snappy)       ← 72 MB vs 342 MB CSV = 4.7x compression
        │
        ▼
  AWS Glue Crawler        ← auto-detects schema, writes to Glue Data Catalog
        │
        ▼
  Amazon Athena           ← serverless SQL over S3; no infrastructure to manage
        │
        ▼
  Power BI Desktop        ← ODBC connection to Athena; 100K rows via direct query
  (4-page dashboard)
```

---

## Tech Stack

| Layer | Tool |
|---|---|
| Storage | Amazon S3 (Parquet + Snappy compression) |
| Catalog | AWS Glue Crawler + Data Catalog |
| Query | Amazon Athena (Presto SQL) |
| Visualization | Power BI Desktop (ODBC via Simba Athena driver) |
| Ingestion | Python (pandas, boto3, pyarrow) |
| Format | Apache Parquet (columnar, 4.7x compression over CSV) |

---

## Dataset

**Source**: [CFPB Consumer Financial Protection Bureau — Public Complaint Database](https://www.consumerfinance.gov/data-research/consumer-complaints/)

- **Full dataset**: 27M rows, 8.3 GB (CSV)
- **Pipeline subset**: 500,000 rows (2023), converted to 72 MB Parquet
- **Columns used**: `product`, `sub_product`, `issue`, `company`, `state`, `date_received`, `timely_response`, `company_response_to_consumer`, `submitted_via`

---

## SQL Queries (Athena)

All queries run serverless against Parquet on S3.

**1. Month-over-Month Growth (LAG window function)**
```sql
WITH monthly AS (
    SELECT SUBSTR("date received", 1, 7) AS month, COUNT(*) AS complaints
    FROM complaints_db.complaints_500k_parquet
    GROUP BY SUBSTR("date received", 1, 7)
)
SELECT month, complaints,
       ROUND((complaints - LAG(complaints) OVER (ORDER BY month)) * 100.0
             / NULLIF(LAG(complaints) OVER (ORDER BY month), 0), 2) AS mom_growth_pct
FROM monthly ORDER BY month;
```

**2. Product Pareto — Cumulative Complaint Share (SUM OVER)**
```sql
WITH pc AS (SELECT product, COUNT(*) AS complaints FROM complaints_db.complaints_500k_parquet GROUP BY product),
ranked AS (
    SELECT product, complaints,
           ROUND(SUM(complaints) OVER (ORDER BY complaints DESC) * 100.0 / SUM(complaints) OVER (), 1) AS cumulative_pct
    FROM pc
)
SELECT product, complaints, cumulative_pct FROM ranked ORDER BY complaints DESC;
```

**3. Company Resolution Quality Score (Weighted CASE WHEN)**
```sql
SELECT company, COUNT(*) AS total_complaints,
       ROUND(SUM(CASE WHEN "company response to consumer" = 'Closed with monetary relief' THEN 3
                      WHEN "company response to consumer" = 'Closed with non-monetary relief' THEN 2
                      WHEN "company response to consumer" = 'Closed with explanation' THEN 1
                      ELSE 0 END) * 100.0 / COUNT(*), 2) AS resolution_score,
       ROUND(SUM(CASE WHEN "timely response?" = 'Yes' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS timely_pct
FROM complaints_db.complaints_500k_parquet
GROUP BY company HAVING COUNT(*) > 500
ORDER BY resolution_score DESC LIMIT 15;
```

**4. Top 3 Issues per Product (ROW_NUMBER PARTITION BY)**
```sql
WITH ic AS (
    SELECT product, issue, COUNT(*) AS complaints
    FROM complaints_db.complaints_500k_parquet GROUP BY product, issue
),
ranked AS (
    SELECT product, issue, complaints,
           ROW_NUMBER() OVER (PARTITION BY product ORDER BY complaints DESC) AS rnk
    FROM ic
)
SELECT product, issue, complaints FROM ranked WHERE rnk <= 3
ORDER BY product, complaints DESC;
```

**5. State Untimely Response Rate**
```sql
SELECT state,
       COUNT(*) AS total,
       ROUND(SUM(CASE WHEN "timely response?" = 'No' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS untimely_pct
FROM complaints_db.complaints_500k_parquet
WHERE state IS NOT NULL
GROUP BY state HAVING COUNT(*) > 1000
ORDER BY untimely_pct DESC LIMIT 15;
```

---

## Power BI Dashboard (4 Pages)

### Page 1 — Executive Overview
- **KPI Cards**: Total Complaints (100K) · Timely Response % (99.6%) · Relief Rate % (35.9%) · Unique Companies (1,371)
- **Decomposition Tree**: Drill path — Total Complaints → Company → State → Product; Wells Fargo leads with 1,634 in FL drill path
- **Key Influencers (AI visual)**: Identifies Debt collection (9.6x), Payday loan (6.8x), and Maine (7.5x) as top drivers of untimely responses

### Page 2 — Key Influencers Detail
- Full Key Influencers panel showing all influencer factors ranked by multiplier with drill-through bar charts

### Page 3 — Geographic & Trend Analysis
- **Heatmap Matrix**: Product × State complaint volume, color-coded by intensity (Credit reporting in FL = 10,082 — darkest cell)
- **Line Chart**: Daily complaint trend (Mar–Apr 2023) with average reference line

### Page 4 — Product Positioning Scatter
- **Scatter Plot**: Each product bubble positioned by Timely Response % (X) vs Relief Rate % (Y), color-coded by product
- Key finding: Mortgage has highest relief rate (~33%); Payday loan has high timely response but low relief

---

## Key Findings

- **Credit reporting dominates**: 52% of all complaints — top product by volume in nearly every state
- **Debt collection is highest-risk**: 9.6x more likely to generate untimely responses vs average
- **FL, CA, TX, GA** are the four highest-volume states across all products
- **Wells Fargo, Experian, TransUnion** are top complaint recipients by company
- **Payday/title loans**: 6.8x untimely response rate despite relatively low volume

---

## Project Structure

```
aws-complaints-pipeline/
├── 01_download_data.py          # Initial data exploration
├── 02_upload_to_s3.py           # Base S3 upload script
├── 02b_sample_and_upload.py     # Date-filtered sample (2023+)
├── 02c_sample_and_upload.py     # CSV 500K row sample
├── 02d_parquet_upload.py        # Final: Parquet + Snappy upload
├── dashboard/
│   └── app.py                   # Streamlit prototype (6 advanced queries)
├── data/                        # Local data (gitignored)
└── README.md
```

---

## AWS Setup

| Resource | Value |
|---|---|
| S3 Bucket | `complaints-pipeline-teja-ohio` (us-east-2) |
| S3 Key | `raw/complaints_500k.parquet` |
| Glue Database | `complaints_db` |
| Glue Table | `complaints_500k_parquet` |
| Athena Output | `s3://complaints-pipeline-teja-ohio/athena-results/` |
| Region | `us-east-2` (Ohio) |

---

## Resume Bullet

> Built a cloud analytics pipeline ingesting 500K CFPB complaint records (S3 → Glue → Athena); wrote 5 advanced SQL queries (LAG, ROW_NUMBER PARTITION BY, cumulative SUM OVER); connected Athena to Power BI via ODBC and built a 4-page dashboard with AI-powered Key Influencers, decomposition tree, and heatmap matrix visuals; reduced storage 4.7x via Parquet/Snappy conversion.

---

*Data source: Consumer Financial Protection Bureau (CFPB) — public domain.*
