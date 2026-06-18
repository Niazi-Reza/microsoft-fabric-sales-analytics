# Microsoft Fabric Sales Analytics

End-to-End Sales Analytics Solution built with Microsoft Fabric and Power BI using the AdventureWorks dataset.

## Project Overview

This project demonstrates a modern Medallion Architecture implementation in Microsoft Fabric:

- Bronze Layer (Raw Data Ingestion)
- Silver Layer (Data Cleansing & Transformation)
- Gold Layer (Business-ready Analytics Model)
- Power BI Semantic Model
- Interactive Sales Dashboard

## Architecture

<img width="151" height="426" alt="Architektur" src="https://github.com/user-attachments/assets/b389effa-5afc-47af-92e9-03a930401c5e" />


## Data Pipeline

### Bronze Layer
Raw source tables loaded into Fabric Lakehouse:

- Product
- Sales
- Reseller
- Region
- Salesperson
- SalespersonRegion
- Targets

### Silver Layer
Data transformation using PySpark notebooks:

- Data type corrections
- Data quality checks
- Duplicate removal
- Standardization
- Audit columns

### Gold Layer
Star Schema implementation:

#### Fact Table
- Fact_Sales

#### Dimension Tables
- Dim_Product
- Dim_Region
- Dim_Reseller
- Dim_Salesperson
- Dim_Date

## Semantic Model

Power BI Direct Lake semantic model built on top of the Gold Layer.

### Measures

- Umsatz
- Kosten
- Gewinn
- Gewinnmarge
- Anzahl Bestellungen
- Durchschnittlicher Umsatz
- Durchschnittliche Verkaufsmenge

## Dashboard

The dashboard provides:

- Sales Overview
- Profit Analysis
- Regional Performance
- Product Category Analysis
- Year Filtering

### Dashboard Screenshot

<img width="1366" height="768" alt="Vertriebsanalyse Dashboard" src="https://github.com/user-attachments/assets/0fc751e1-7c4a-48ce-8584-a60be980bad9" />

### Data Model

 <img width="1366" height="768" alt="Semantic_Modell" src="https://github.com/user-attachments/assets/fa836ce7-3961-4ff7-8664-b13f9fce2021" />


## Technologies

- Microsoft Fabric
- Lakehouse
- PySpark
- Power BI
- Direct Lake
- GitHub
- SQL

## Repository Structure

```text
microsoft-fabric-sales-analytics/
│
├── notebooks/
│   ├── etl_Bronze_layer.ipynb
│   ├── etl_Silver_layer.ipynb
│   └── etl_Gold_layer.ipynb
│
├── screenshots/
│   ├── dashboard.png
│   ├── semantic-model.png
│   └── architecture.png
│
└── README.md
