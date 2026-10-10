# 🍕 Zomato AI Data Engineering — End-to-End Batch Pipeline

[![Python Version](https://img.shields.io/badge/python-3.13%2B-blue)](https://www.python.org/)
[![dbt](https://img.shields.io/badge/dbt-snowflake%201.12%2B-orange)](https://www.getdbt.com/)
[![Snowflake](https://img.shields.io/badge/Snowflake-Data%20Warehouse-00a1e9)](https://www.snowflake.com/)
[![Airflow](https://img.shields.io/badge/Apache%20Airflow-3.0-green)](https://airflow.apache.org/)
[![OpenAI](https://img.shields.io/badge/OpenAI-GPT--4o--mini%20%7C%20Embeddings-purple)](https://openai.com/)
[![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-red)](https://streamlit.io/)

---

## 📌 Overview

An end-to-end modern data engineering project building a scalable batch data pipeline for Zomato-style food delivery data. The pipeline extracts raw data from CSVs in **Amazon S3**, ingests it into **Snowflake** (Bronze), transforms it with **dbt** into Silver and Gold layers using Medallion Architecture, enriches unstructured review data using **OpenAI LLMs**, and orchestrates the complete flow daily with **Apache Airflow 3** on Docker. Streamlit apps provide RAG-based search ("Chat with your reviews") and Text-to-SQL warehouse querying.

> 🎥 **Video Tutorial Reference:** [Watch the full project walkthrough on YouTube](https://youtu.be/kYwaNMQ3XT8?si=Ge8ilVxkmGQS6iIg)  
> 📂 **Original Repository:** [darshilparmar/zomato-ai-data-engineering-end-to-end-project](https://github.com/darshilparmar/zomato-ai-data-engineering-end-to-end-project)

---

## 🏗️ Architecture

```
[Raw CSVs] ──> [Amazon S3 Lake] ──> [Snowflake RAW (Bronze)] 
                                           │
                                     (dbt Build)
                                           │
                                           ▼
                                 [Snowflake STAGING (Silver)]
                                           │
                                     (dbt Build)
                                           │
                                           ▼
                                  [Snowflake MARTS (Gold)]
                                           │
                                   (OpenAI Enrichment)
                                           │
                                           ▼
                             [Snowflake AI Layer & Streamlit Apps]
```

### Medallion Data Architecture

| Layer | Environment / Schema | Description & Details |
| :--- | :--- | :--- |
| **Source** | `data/` (Local / S3) | Raw dimension CSVs (`restaurants`, `users`, `food`, `menu`) and fact CSVs (`orders`, `order_items`, `reviews`). |
| **Lake** | Amazon S3 | S3 bucket hosting raw CSVs partitioned under `raw/<table>/`. |
| **Bronze (Raw)** | `ZOMATO.RAW` | Direct ingestion via Snowflake keyless S3 Storage Integration and `COPY INTO` queries. |
| **Silver (Staging)** | `ZOMATO.STAGING` | 7 dbt staging views cleaning, casting data types, renaming columns, and enforcing schema standards. |
| **Gold (Marts)** | `ZOMATO.MARTS` | Dimensional tables (`dim_customer`, `dim_restaurants`, `dim_food`, `dim_date`), incremental facts (`fct_orders`, `fct_order_items`), and business analytical marts (`mart_daily_city_revenue`, `mart_delivery_sla`, `mart_restaturant_performance`, `mart_review_insights`). |
| **AI Layer** | `ZOMATO.AI` | Sentiment, topic extraction, and summary enrichment (`REVIEW_ENRICHED`) powered by OpenAI API, plus RAG chat and Text-to-SQL querying. |
| **Orchestration** | Apache Airflow 3 | Daily automated DAG execution in Docker (`zomato_batch`). |

---

## 📁 Repository Structure

```
zomato-data-pipeline/
├── ai/                              # AI Enrichment & Streamlit Interface Layer
│   ├── enrich_reviews.py            # LLM review enrichment script (writes to ZOMATO.AI.REVIEW_ENRICHED)
│   ├── rag_chat.py                  # RAG Streamlit app ("Chat with your reviews")
│   ├── text_to_sql.py               # Text-to-SQL Streamlit app ("Chat with your warehouse")
│   ├── review_embeddings_cache.parquet # Pre-calculated embeddings cache for fast retrieval
│   └── .env                         # Local environment configuration for AI tools
├── airflow/                         # Apache Airflow 3 Orchestration (Docker)
│   ├── Dockerfile                   # Custom Airflow image with dbt-snowflake & OpenAI venv
│   ├── docker-compose.yml           # Airflow service definitions (scheduler, webserver, postgres)
│   ├── .env                         # Airflow environment variables & credentials
│   └── dags/
│       └── zomato_batch.py          # Master Airflow DAG (4 sequential execution steps)
├── zomato/                          # dbt Transformation Project
│   ├── models/
│   │   ├── staging/                 # Staging models (Silver layer): 7 staging SQL files + YAMLs
│   │   └── marts/                   # Gold marts & dimension models: facts, dims, and aggregated marts
│   ├── macros/                      # Custom dbt macros (e.g. custom schema naming)
│   ├── dbt_project.yml              # dbt project configuration
│   └── profiles.yml                 # Snowflake target profile configuration
├── main.py                          # Entry script
├── pyproject.toml                   # Project dependencies and python project config
├── uv.lock                          # Dependency lockfile (uv package manager)
└── README.md                        # Project documentation
```

---

## ⚡ Tech Stack & Tools

- **Language & Runtime:** Python 3.13+, `uv` package manager
- **Data Lake Storage:** Amazon S3
- **Data Warehouse:** Snowflake (Storage Integrations, External Stages, Copy Into, Views, Tables)
- **Data Transformation:** dbt (`dbt-snowflake`)
- **Pipeline Orchestration:** Apache Airflow 3 (Docker Compose)
- **AI & LLM Services:** OpenAI (`gpt-4o-mini`, `text-embedding-3-small`)
- **Frontend / Apps:** Streamlit

---

## 🔄 Airflow Pipeline Workflow (`zomato_batch`)

The pipeline runs daily (`@daily`) in Apache Airflow through four linked tasks:

1. **`reload_raw`** (`SQLExecuteQueryOperator`)  
   Executes Snowflake `COPY INTO` commands to load fresh CSV files from S3 into `ZOMATO.RAW` bronze tables.
2. **`dbt_build_core`** (`BashOperator`)  
   Executes `dbt build --exclude tag:ai` to transform raw data into staging views and core marts.
3. **`enrich_reviews`** (`BashOperator`)  
   Runs `ai/enrich_reviews.py` to process unstructured reviews via OpenAI GPT-4o-mini, generating structured sentiment scores, topics, and summaries into `ZOMATO.AI.REVIEW_ENRICHED`.
4. **`dbt_build_ai`** (`BashOperator`)  
   Executes `dbt build --select tag:ai` to build downstream AI aggregate marts (`mart_review_insights`).

---

## 🚀 Setup & Execution Guide

### 1. Prerequisites
- Python 3.13+
- Docker & Docker Desktop
- Snowflake account
- AWS account with S3 access
- OpenAI API Key

### 2. Virtual Environment & Dependencies
Manage dependencies using `uv`:
```bash
# Sync dependencies with uv
uv sync
```
Or with standard `pip`:
```bash
pip install dbt-snowflake openai python-dotenv snowflake-connector-python[pandas] streamlit
```

### 3. Snowflake Setup
Run the setup SQL scripts in Snowsight in sequence (01 to 05):
1. **Warehouse & Database Creation**: Create `ZOMATO_WH`, database `ZOMATO`, schemas (`RAW`, `STAGING`, `MARTS`, `AI`), and roles.
2. **Storage Integration**: Set up keyless AWS IAM role authentication between AWS S3 and Snowflake.
3. **External Stage & File Format**: Create `ZOMATO_RAW_STAGE` pointing to your S3 bucket.
4. **Raw Tables DDL**: Create bronze tables matching raw CSV columns.
5. **Initial Data Load**: Execute `COPY INTO` queries.

### 4. Running dbt Locally
Navigate to the dbt project folder:
```bash
cd zomato

# Test Snowflake connectivity
dbt debug

# Build dbt models
dbt build
```

### 5. Running Apache Airflow (Docker)
Navigate to the `airflow/` folder:
```bash
cd airflow

# Configure your environment variables in .env
# Start Airflow container stack
docker-compose up -d
```
Access the Airflow UI at `http://localhost:8080`.

### 6. Running AI Streamlit Apps
In the `ai/` folder, ensure your `.env` contains your `OPENAI_API_KEY` and Snowflake credentials:

**RAG Review Chat App:**
```bash
streamlit run ai/rag_chat.py
```

**Text-to-SQL Warehouse Query App:**
```bash
streamlit run ai/text_to_sql.py
```

---

## 📊 Business Marts & Insights Built

- **`mart_daily_city_revenue`**: Daily revenue and order volume breakdown by city.
- **`mart_delivery_sla`**: Delivery SLA compliance, average delivery time, and delays.
- **`mart_restaturant_performance`**: Total revenue, rating, order count, and customer retention per restaurant.
- **`mart_review_insights`**: Aggregated review analysis combining LLM-extracted sentiment and key topics with sales data.

---