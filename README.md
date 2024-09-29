# Formula-1-pipeline

## **Introduction**

In this **Databricks** project, I aimed to learn how to write **PySpark** code and explore the use of **Delta tables**, which support ACID transactions, incremental loading, and time travel. The project was centered around Formula 1 data.

### **Key Technologies:**

- Delta tables (ACID, incremental loading, time travel)
- PySpark for data transformation
- SQL for querying and analysis

---

## **Requirements**

### **Preparation and Transformation**

- Include schema
- Able to analyze via SQL
- Stored in parquet format
- Support incremental load
- Ability to roll back to a previous version

### **Reporting**

- Driver Standing
- Constructor Standing

### **Non-functional**

- Scheduled to run every Sunday 10PM
- Ability to monitor pipeline

---

## **Data overview**

### **Data Sources:**  
Eight different data files (circuits, races, constructors, drivers, results, pitstops, laptimes, and qualifying) with various file formats (CSV, JSON).

### **Data Folders:**

- Data was organized into three folders:
  - '2021-03-21' containing all historical race data
  - '2021-03-28' including the first race in 2021
  - '2021-04-18' containing the second race in 2021

---

## **Process Outline**

### **Solution Architecture**

**\[img\]**

- **External vs. Managed Tables:**
  - **External Tables:** Bronze layer tables were set as external to retain the data even if the table is dropped.
  - **Managed Tables:** Silver and gold layers utilized managed tables, so data is deleted when the table is dropped.
- **Full vs. Incremental Load:**
  - **Full Load:** Applied to circuits, races, constructors, and drivers in silver and gold layer, which are less frequently updated.
  - **Incremental Load:** Implemented for results, pitstops, laptimes, and qualifying in silver and gold layer, which are frequently updated.

### **Raw**

Create External Table: The raw data was ingested by creating an external table to ensure that when the table is dropped, the data is retained. The table path and location were specified explicitly.

### **Ingestion**

**\[img\]**

- **Reading Data:** All raw data files were read into a DataFrame using **spark.read()**.
- **Data Preparation Steps:**
  - **Schema Specification:** The schema was defined using **StructType**.
  - **Column Selection:** Specific columns were selected using **df.select().**
  - **Column Renaming:** Columns were renamed using **df.withColumnRenamed()**.
  - **New Column Creation:** Additional columns were created using **df.withColumn()**.
- **Parameter Handling:**
  - Parameters were created using **dbutils.widgets.text()** for dynamic data processing.
  - Example:
    - Created parameter: p_file_source
    - The parameter value was fetched using **dbutils.widgets.get()** and assigned to a variable v_file_date.
    - A new column called "file date" was added based on the parameter value.
- **Writing Data:**
- **Full Load:**
  - Data that required a full load was written directly into a Delta table as a managed table, without specifying the location.
- **Incremental Load:**
  - Data that required incremental loading was written into a Delta table. A merge statement was used to either update or insert data based on whether the table already existed.
  - **Merge Logic:**
    - **If Table Exists:** The data was updated using the merge into syntax.
    - **If Table Does Not Exist:** A new Delta table was created using the relevant syntax.

### **Transformation**

**Race Result:**

- **Reading Data**: Data was read from the silver layer.
- **Joining Data**: The data from circuits, drivers, constructors, races, and results was joined using the **df.join** function.
- **Parameter Handling**: Parameters were created dynamically using dbutils.widgets.text() for date-based operations:
- **Writing Data**: Incremental load data was updated or inserted using the **merge** function.

**Driver and Constructor standings**

- **Reading Data:** The race result data was read from the gold layer.
- **Aggregation:**
  - **Driver Standings:**  
        The data was grouped by race year, driver, nationality, and team. Points were summed, and the number of wins was counted using the **.groupBy(), .sum(), and .count()** functions.
  - **Constructor Standings:** 
        Similarly, constructor standings were aggregated by race year and team, with points summed and wins counted using the **.groupBy(), .sum(), and .count()** functions.
- **Window Functions for Ranking:**
  - **Driver Standings:**  
        Drivers were ranked based on total points and win counts using the **rank() over()** window function.
  - **Constructor Standings:**  
        Constructors were ranked using the same method, with rankings based on total points and number of wins.
- **Parameter Handling**: Parameters were created dynamically using dbutils.widgets.text() for date-based operations:
- **Writing Data:** Incremental load data was updated or inserted using the **merge** function.

### **Data pipeline**

**Data Pipeline Creation:**

- **Integration with Databricks:**
  - A data pipeline was created in Azure Data Factory (ADF), integrating with Databricks notebooks for data processing and transformation.

**Scheduling:**

- **Tumbling Window Trigger:**
  - The pipeline was scheduled to run automatically using a tumbling window trigger, which executes the pipeline at a fixed interval.
  - **Schedule Frequency:** The pipeline is configured to run every week, specifically on Sunday at 10 PM.

**\[img\]**

---

## **Learning Outcome**

### **1\. Proficiency in PySpark:**

- **Data Manipulation:** Developed strong skills in reading, processing, and transforming large datasets using PySpark. This included defining schemas, selecting and renaming columns, and creating new columns for metadata.
- **Joins and Aggregations:** Mastered functions such as df.join(), .groupBy(), .sum(), and .count() to join datasets and perform various aggregations, such as calculating driver and constructor standings.

### **2\. Implementation of Delta Lake:**

- **ACID Transactions:** Delta tables enabled me to work with ACID (Atomicity, Consistency, Isolation, Durability) transactions, ensuring data consistency and integrity throughout the entire pipeline.
- **Version Control:** I learned to manage different versions of data using Delta Lake, which allowed for easy rollbacks and time travel to previous states of the data.
- **Time Travel:** I gained practical knowledge of how Delta Lake's versioning feature enables time travel, providing the ability to analyze and restore historical data versions.

### **3\. Knowledge of Incremental Load:**

- **Incremental Data Processing:** I developed expertise in implementing incremental loads, processing only new or updated data rather than reprocessing entire datasets. This approach was crucial for frequently updated data such as race results, pitstops, laptimes, and qualifying.
- **Merge Operations:** I applied the merge operation in Delta Lake to efficiently handle insert, update, or delete records, improving the performance and efficiency of data pipelines.
- **Optimization of Full vs. Incremental Loads:** I learned when to apply full loads for static datasets (e.g., circuits, races, constructors) and incremental loads for dynamic datasets, reducing unnecessary processing and improving system performance.

### **4\. Knowledge of Managed Tables and External Tables:**

- **Managed Tables:** Gained an understanding of managed tables in Delta Lake, where the system controls the data's lifecycle. This was applied to the silver and gold layers, where data is automatically deleted when the table is dropped, making it useful for clean and controlled environments.
- **External Tables:** I learned the advantages of external tables, where the data is stored outside the control of the Delta table and remains even when the table is dropped. This was crucial for the bronze layer, where data persistence was important for long-term storage and reuse.
