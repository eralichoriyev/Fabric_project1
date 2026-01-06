# Microsoft Fabric Medallion Architecture Project

## Overview
This project implements a Medallion Architecture (Bronze, Silver, Gold) using Microsoft Fabric.

## Data Sources
- OpenAQ API (Air Quality data)
- World Bank API (GDP data)
- ECB Exchange Rate dataset
- NYC Taxi public dataset

## Architecture
- **Bronze**: Raw API responses stored as JSON/CSV
- **Silver**: Cleaned and flattened datasets using PySpark
- **Gold**: Analytics-ready tables for reporting

## Tools Used
- Microsoft Fabric
- Lakehouse
- PySpark Notebooks
- Spark SQL

## Notes
Due to regional restrictions, Dataflow Gen2 was unavailable. All ingestion and transformations were implemented using Fabric Notebooks.

## Outcome
Fully functional medallion architecture with multiple external data sources and analytics-ready Gold tables.
