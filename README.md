# multinational-retail-data-centralisation646
# Multinational Retail Data Centralisation

This repository contains a data engineering pipeline for a multinational retailer. The goal is to unify sales data from multiple sources into a single Postgres database (sales_data), facilitating analytics and business insights.

## Table of Contents
1. [Overview](#overview)
2. [Installation](#installation)
3. [Usage](#usage)
4. [File Structure](#file-structure)
5. [License](#license)

## Overview
- **What it does**: Extracts data from AWS RDS, S3 buckets (CSV/JSON), an API, and PDFs, then cleans the data before loading into a local Postgres database.  
- **Why**: To have a single source of truth for sales data, enabling better reporting and analytics.  
- **What I learned**: Setting up a conda environment, connecting to Postgres with psycopg2 and SQLAlchemy, data cleaning with pandas, and reading from various data sources (API, PDF, CSV, JSON).

## Installation
1. Clone this repository:  
   ```bash
   git clone https://github.com/auwalmusa/multinational-retail-data-centralisation646.git


Create/activate a conda environment (Python 3.12+):
bash
Copy
Edit
conda create -n aicore_project python=3.12
conda activate aicore_project
Install dependencies:
bash
Copy
Edit
pip install -r requirements.txt
(Or manually install: pandas, requests, tabula-py, psycopg2-binary, PyYAML, sqlalchemy, etc.)
Usage
Ensure local Postgres is running and credentials are stored in local_creds.yaml.
Run each main_*.py script to extract, clean, and load data. For example:
bash
Copy
Edit
python main.py          # to load user data
python main_stores.py   # to load store data
python main_products.py # to load products data
python main_orders.py   # to load orders data
python main_date_times.py # to load date/time data
Check sales_data database in Postgres to verify loaded tables (dim_users, dim_store_details, dim_products, orders_table, dim_date_times).
File Structure
bash
Copy
Edit
multinational-retail-data-centralisation646/
├─ data_extraction.py     # Classes/methods to extract data from RDS, S3, PDF, API
├─ data_cleaning.py       # Classes/methods to clean user, card, store, orders, product data
├─ database_utils.py      # Class to connect & upload data to Postgres
├─ main.py                # Orchestrates user data extraction -> cleaning -> loading
├─ main_stores.py         # For store data pipeline
├─ main_products.py       # For product data pipeline
├─ main_orders.py         # For orders data pipeline
├─ main_date_times.py     # For date/times data pipeline
├─ db_creds.yaml          # RDS credentials (in .gitignore)
├─ local_creds.yaml       # Local DB credentials (in .gitignore)
└─ README.md
License
MIT License (if applicable)

sql
Copy
Edit

Remember to commit this **README.md** file:
```bash
git add README.md
git commit -m "Update README with project details"
git push