# 📈 Real-Time Financial Data Pipeline
A scalable, modular, and asynchronous financial data pipeline built with Python, FastAPI, Pandas, and Parquet, designed to ingest, transform, and serve real-time financial market data from multiple sources.

## 🚀 Project Overview
This project automates the collection, transformation, and storage of financial data such as stock prices, forex, cryptocurrency, and economic indicators from multiple public APIs and CSV files.

The data is cleaned, normalized, and stored in a layered Data Lake architecture (Bronze, Silver, Gold) using Parquet files, enabling efficient querying and integration with analytics dashboards or machine learning models.

## 🔧 Features
 ✅ Multi-source data ingestion: Alpha Vantage, Yahoo Finance, CSVs  
 ✅ Data transformation: Cleaning, normalization, and aggregation with Pandas  
 ✅ Asynchronous data ingestion using FastAPI’s BackgroundTasks  
 ✅ Layered Data Lake (Bronze → Silver → Gold) for data versioning and quality  
 ✅ Scalable & modular design for easy maintenance and extension  
 ✅ Retry logic with exponential backoff for handling API rate limits  
 ✅ REST API to access transformed data in real-time  
 ✅ Jinja2 templating for dynamic HTML views (optional)

 ## 🧰 Tech Stack
1.Python
2.FastAPI
3.Pandas
4.Parquet
5.Jinja2 (for optional frontend templates)
6.Uvicorn (for FastAPI deployment)
7.Alpha Vantage API, Yahoo Finance, CSV

## 📂 data_lake/
bronze is Raw ingested data

silver is Cleaned & normalized data

gold is Aggregated and ready-for-analysis data

## 🔄 Pipeline Flow
1.Ingestion: Retrieve data from APIs/CSVs asynchronously

2.Transformation: Clean and standardize using Pandas

3.Storage: Write structured data to Parquet files in layered lake

4.API Access: Expose processed data via FastAPI endpoints

## 📌 Use Cases
1.Real-time trading & quantitative finance research

2.Financial dashboards & data visualizations

3.Market trend analysis & decision-making tools

## ⚠️ Challenges Overcome
1.Handling API rate limits with retry + exponential backoff

2.Managing format inconsistencies in CSVs

3.Building robust, modular, and asynchronous components

4.Designing scalable Data Lake architecture

## 🧠 Key Learnings
1.Asynchronous programming with FastAPI

2.Scalable data pipeline architecture

3.Efficient storage with Parquet

4.Robust error handling and retry mechanisms

5.API integration and clean REST API design

## 📸 Architecture Diagram

<img width="945" alt="image" src="https://github.com/user-attachments/assets/79a1af57-c260-4b58-8248-f60f25a0108f" />


