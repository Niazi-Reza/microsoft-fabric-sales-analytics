# Gold Layer 
### Import Libraries
# Import Libraries

from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import col
### DIM_PRODUCT
# DIM_PRODUCT 

## Lesen
df_product = spark.read.table("Silver_Lakehouse.dbo.Silver_Product")


## Transforamtion
dim_product = (
    df_product
    .select(
        "ProductKey",
        "Product",
        "Color",
        "Subcategory",
        "Category",
        "Standard_Cost",
        "load_timestamp",
        "source_system"
    )
    .dropDuplicates(["ProductKey"])
    .withColumn("ProductID", monotonically_increasing_id() + 1)
)
## Surrogate Key nach vorne
dim_product = dim_product.select(
    "ProductID",
    "ProductKey",
    "Product",
    "Color",
    "Subcategory",
    "Category",
    "Standard_Cost",
    "load_timestamp",
    "source_system"
)
## display
display(dim_product.limit(10))
##  DIM_PRODUCT In Gold Layer speichern
dim_product.write.mode("overwrite").saveAsTable("Gold_Lakehouse.dbo.Dim_Product")
### DIM_REGION
# DIM_REGION

## Lesen
df_region = spark.read.table("Silver_Lakehouse.dbo.Silver_Region")

dim_region = (
    df_region
    .select(
        "SalesTerritoryKey",
        "Region",
        "Country",
        "Group",
        "load_timestamp",
        "source_system"
    )
    .dropDuplicates()
    .withColumn("RegionID", monotonically_increasing_id() + 1)
)

## Surrogate Key nach vorne
dim_region = dim_region.select(
    "RegionID",
    "SalesTerritoryKey",
    "Region",
    "Country",
    "Group",
    "load_timestamp",
    "source_system"
)

display(
    spark.read.table("Gold_Lakehouse.dbo.Dim_Region").limit(10)
)
## DIM_REGION in Gold Layer speichern
dim_region.write.mode("overwrite").saveAsTable("Gold_Lakehouse.dbo.Dim_Region")
### DIM_RESELLER
# DIM_RESELLER

## Reserller Lesen
df_reseller = spark.read.table("Silver_Lakehouse.dbo.Silver_Reseller")

dim_reseller = (
    df_reseller
    .select(
        "ResellerKey",
        "Reseller",
        "Business_Type",
        "City",
        "State_Province",
        "Country_Region",
        "load_timestamp",
        "record_source"
    )
    .dropDuplicates()
    .withColumn("ResellerID", monotonically_increasing_id() + 1)
)

## Surrogate Key nach vorne
dim_reseller = dim_reseller.select(
    "ResellerID",
    "ResellerKey",
    "Reseller",
    "Business_Type",
    "City",
    "State_Province",
    "Country_Region",
    "load_timestamp",
    "record_source"
)

display(dim_reseller.limit(10))
## DIM_RESELLER in Gold Layer speichern
dim_reseller.write.mode("overwrite").saveAsTable("Gold_Lakehouse.dbo.Dim_Reseller")
### DIM_SALESPERSON
# GalesPerson Dimension erstellen
## SalesPerson lesen
df_salesperson = spark.read.table("Silver_Lakehouse.dbo.Silver_Salesperson")

dim_salesperson = (
    df_salesperson
    .select(
        "EmployeeKey",
        "EmployeeID",
        "Salesperson",
        "Title",
        "UPN",
        "load_timestamp",
        "record_source"
    )
    .dropDuplicates()
    .withColumn("SalespersonID", monotonically_increasing_id() + 1)
)

## Surrogate Key nach vorne
dim_salesperson = dim_salesperson.select(
    "SalespersonID",
    "EmployeeKey",
    "EmployeeID",
    "Salesperson",
    "Title",
    "UPN",
    "load_timestamp",
    "record_source"
)

display(dim_salesperson.limit(10))
## DIM_SalesPerson in Gold Layer speichern
dim_salesperson.write.mode("overwrite").saveAsTable("Gold_Lakehouse.dbo.Dim_Salesperson")
### FACT_SALES
# FACT_SALES 

## Lesen
df_sales = spark.read.table("Silver_Lakehouse.dbo.Silver_Sales")

dim_product = spark.read.table("Gold_Lakehouse.dbo.Dim_Product")
dim_region = spark.read.table("Gold_Lakehouse.dbo.Dim_Region")
dim_reseller = spark.read.table("Gold_Lakehouse.dbo.Dim_Reseller")
dim_salesperson = spark.read.table("Gold_Lakehouse.dbo.Dim_Salesperson")


## fact table befüllen
fact_sales = (
    df_sales
    .join(dim_product.select("ProductKey", "ProductID"), "ProductKey", "left")
    .join(dim_reseller.select("ResellerKey", "ResellerID"), "ResellerKey", "left")
    .join(dim_salesperson.select("EmployeeKey", "SalespersonID"), "EmployeeKey", "left")
    .join(dim_region.select("SalesTerritoryKey", "RegionID"), "SalesTerritoryKey", "left")
    .select(
        "SalesOrderNumber",
        "OrderDate",
        "ProductID",
        "ResellerID",
        "SalespersonID",
        "RegionID",
        "Quantity",
        "Unit_Price",
        "Sales",
        "Cost",
        "load_timestamp",
        "record_source"
    )
)

display(fact_sales.limit(10))
## fact Sales in Gold Lazer speichern
fact_sales.write.mode("overwrite").saveAsTable(
    "Gold_Lakehouse.dbo.Fact_Sales"
)
### DIM_DATE
from pyspark.sql.functions import (
    col,
    year,
    quarter,
    month,
    dayofmonth,
    date_format,
    monotonically_increasing_id
)

df_sales = spark.read.table("Silver_Lakehouse.dbo.Silver_Sales")

dim_date = (
    df_sales
    .select("OrderDate")
    .dropDuplicates()
    .withColumn("DateID", monotonically_increasing_id() + 1)
    .withColumn("Year", year(col("OrderDate")))
    .withColumn("Quarter", quarter(col("OrderDate")))
    .withColumn("Month", month(col("OrderDate")))
    .withColumn("MonthName", date_format(col("OrderDate"), "MMMM"))
    .withColumn("Day", dayofmonth(col("OrderDate")))
)

dim_date = dim_date.select(
    "DateID",
    "OrderDate",
    "Year",
    "Quarter",
    "Month",
    "MonthName",
    "Day"
)

display(dim_date.limit(10))
dim_date.write.mode("overwrite").saveAsTable("Gold_Lakehouse.dbo.DIM_DATE")