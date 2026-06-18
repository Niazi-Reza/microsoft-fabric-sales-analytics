# Silver Layer
### Import Libraries 
##

from pyspark.sql.functions import (
    current_timestamp,
    lit,
    col,
    to_date,
    regexp_replace
)

from pyspark.sql.types import (
    IntegerType,
    DecimalType,
    DateType
)


### Bronze Product 
# 1.1 Bronze_Product lesen
df_product = spark.read.table("Bronze_Lakehouse.dbo.Bronze_Product")


# 1.2 Trasformacodes

df_product_silver = (
    df_product
    .withColumnRenamed("Standard Cost", "Standard_Cost")
    .withColumnRenamed("Background Color Format", "Background_Color_Format")
    .withColumnRenamed("Font Color Format", "Font_Color_Format")
    .fillna({
        "Product": "Unknown",
        "Color": "Unknown",
        "Subcategory": "Unknown",
        "Category": "Unknown",
        "Background_Color_Format": "Unknown",
        "Font_Color_Format": "Unknown"
    })
    .withColumn("load_timestamp", current_timestamp())
    .withColumn("source_system", lit("Bronze_Layer"))
    .dropDuplicates()
)
display(df_product_silver.limit(10))

# 1.3 In Silver Layer speichern
df_product_silver.write.mode("overwrite").saveAsTable("Silver_Product")

### Bronze Region

# 2.1 Bronze_Region lesen
df_region = spark.read.table("Bronze_Lakehouse.dbo.Bronze_Region")

# 2.2 Transformcodes                
df_region_silver = (
    df_region
    .fillna({
        "Region": "Unknown",
        "Country": "Unknown",
        "Group": "Unknown"
    })
    .withColumn("load_timestamp", current_timestamp())
    .withColumn("source_system", lit("Bronez_Layer"))
    .dropDuplicates()
)


display(df_region_silver.limit(10))

# 2.3 In Siler Layer speichern
df_region_silver.write.mode("overwrite").saveAsTable("Silver_Region")

### Bronze Reseller lesen
# 3.1 Bronze Reseller Lesen     
df_reseller = spark.read.table("Bronze_Lakehouse.dbo.Bronze_Reseller")

# 3.2 Transoformcodes
df_reseller_silver = (
    df_reseller
    .withColumnRenamed("Business Type", "Business_Type")
    .withColumnRenamed("State-Province", "State_Province")
    .withColumnRenamed("Country-Region", "Country_Region")
    .fillna({
        "Reseller": "Unknown",
        "Business_Type": "Unknown",
        "City": "Unknown",
        "State_Province": "Unknown",
        "Country_Region": "Unknown"
    })
    .withColumn("load_timestamp", current_timestamp())
    .withColumn("record_source", lit("Bronze_Layer"))
    .dropDuplicates()
)

display(df_reseller_silver.limit(10))


# 3.3 I Silver Layer speichern
df_reseller_silver.write.mode("overwrite").saveAsTable("Silver_Reseller")
### Bronze Sales 
# 4.1 Bronze Sales Lesen
df_sales = spark.read.table("Bronze_Lakehouse.dbo.Bronze_Sales")
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
# 4.2 Transformcodes
df_sales_silver = (
    df_sales
    .withColumnRenamed("Unit Price", "Unit_Price")

    # Datentypen
    .withColumn("OrderDate", to_date(col("OrderDate"), "EEEE, MMMM d, yyyy"))
    .withColumn("ProductKey", col("ProductKey").cast(IntegerType()))
    .withColumn("ResellerKey", col("ResellerKey").cast(IntegerType()))
    .withColumn("EmployeeKey", col("EmployeeKey").cast(IntegerType()))
    .withColumn("SalesTerritoryKey", col("SalesTerritoryKey").cast(IntegerType()))
    .withColumn("Quantity", col("Quantity").cast(IntegerType()))

    # Währungsfelder bereinigen und casten
    .withColumn("Unit_Price",regexp_replace(regexp_replace(col("Unit_Price"), "\\$", ""),",","").cast(DecimalType(18, 2)))
    .withColumn("Sales",regexp_replace(regexp_replace(col("Sales"), "\\$", ""),",","").cast(DecimalType(18, 2)))
    .withColumn("Cost",regexp_replace(regexp_replace(col("Cost"), "\\$", ""),",","").cast(DecimalType(18, 2))
    )

    # Null Handling
    .fillna({"Quantity": 0,"Unit_Price": 0,"Sales": 0,"Cost": 0})

    # Audit Spalten
    .withColumn("load_timestamp", current_timestamp())
    .withColumn("record_source", lit("Bronze_Layer"))

    # Deduplikation
    .dropDuplicates()
)

display(df_sales_silver.limit(10))

# 4.3 In Silver Layer speichern
df_salesperson = spark.read.table("Bronze_Lakehouse.dbo.Bronze_Salesperson")

df_salesperson_silver = (
    df_salesperson
    .withColumn("EmployeeKey", col("EmployeeKey").cast(IntegerType()))
    .fillna({
        "EmployeeID": "Unknown",
        "Salesperson": "Unknown",
        "Title": "Unknown",
        "UPN": "Unknown"
    })
    .withColumn("load_timestamp", current_timestamp())
    .withColumn("record_source", lit("Bronze_Layer"))
    .dropDuplicates()
)

display(df_salesperson_silver.limit(10))
df_salesperson_silver.write.mode("overwrite").saveAsTable("Silver_Salesperson")
  ### SalesPersonRegion
# 5.1 Bronze SalesPersonRegion Lesen
df_salesperson_region = spark.read.table("Bronze_Lakehouse.dbo.Bronze_SalespersonRegion")

# 5.2 Transformcodes
df_salesperson_region_silver = (
    df_salesperson_region
    .withColumn("EmployeeKey", col("EmployeeKey").cast(IntegerType()))
    .withColumn("SalesTerritoryKey", col("SalesTerritoryKey").cast(IntegerType()))
    .withColumn("load_timestamp", current_timestamp())
    .withColumn("record_source", lit("Bronze_Layer"))
    .dropDuplicates()
)

display(df_salesperson_region_silver.limit(10))
# 5.3 In Silver Layer speichern
df_salesperson_region_silver.write.mode("overwrite").saveAsTable("Silver_SalespersonRegion")                
### Broneze_Target 
# 6.1 Broneze Target Lesen
df_targets = spark.read.table("Bronze_Lakehouse.dbo.Bronze_Targets")

# 6.2 Transformacodes
df_targets_silver = (
    df_targets
    .withColumn("EmployeeID", col("EmployeeID").cast(IntegerType()))
    .withColumn("Target", col("Target").cast(DecimalType(18,2)))
    .fillna({
        "Target": 0
    })
    .withColumn("load_timestamp", current_timestamp())
    .withColumn("record_source", lit("Bronze_Layer"))
    .dropDuplicates()
)

display(df_targets_silver.limit(10))
# 6.3 In Silver Layer speichern
df_targets_silver.write.mode("overwrite").saveAsTable("Silver_Targets")