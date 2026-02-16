# 🚀 Spark Parquet & Delta Lake Demo (Java)

A hands-on Apache Spark (Java) project showcasing Parquet operations and Delta Lake features including ACID transactions, updates, history, and time travel.

## 🔥 Features

* ✅ Writing & Reading Parquet files
* ✅ Generating 1 Million Records (Distributed)
* ✅ Delta Lake Table Creation
* ✅ Append Operations
* ✅ Update Operations
* ✅ Delta Table History
* ✅ Time Travel (Version-based Reads)

Built using **Apache Spark 3.5.1** and **Delta Lake 3.1.0** with Java.

---

## 🧱 Tech Stack

* **Java 8**
* **Apache Spark 3.5.1**
* **Delta Lake 3.1.0**
* **Maven**

---

## 📂 Project Structure

```
spark-parquet-delta-demo/
│
├── src/main/java/com/oggu/spark/parqt/
│   ├── SparkWriteParquet.java
│   ├── SparkReadParquet.java
│   ├── GenerateMillionRecords.java
│
├── src/main/java/com/oggu/spark/delta/
│   ├── Person.java
│   ├── DeltaExample.java
│   ├── DeltaAppendTimeTravel.java
│   ├── DeltaUpdateTimeTravel.java
│
└── pom.xml
```

---

# 🟦 PART 1 — Parquet Examples

## 1️⃣ Write Parquet

`SparkWriteParquet.java`

* Creates a small dataset
* Writes to:

  ```
  output/parquet_data
  ```
* Uses overwrite mode

---

## 2️⃣ Read Parquet

`SparkReadParquet.java`

* Reads the previously written parquet file
* Displays:

  * Data
  * Schema

---

## 3️⃣ Generate 1 Million Records

`GenerateMillionRecords.java`

* Uses `spark.range()` for distributed data generation
* Creates 1,000,000 rows
* Writes compressed parquet (Snappy)
* Measures write time

Example output:

```
Partitions: 8
Write time (seconds): 3.42
```

This demonstrates:

* Distributed data generation
* Partition handling
* Write performance measurement

---

# 🟪 PART 2 — Delta Lake Examples

Delta Lake adds **ACID transactions**, **versioning**, and **time travel** on top of Parquet.

---

## 1️⃣ Basic Delta Table

`DeltaExample.java`

* Writes dataset as Delta table:

  ```
  output/delta-table
  ```
* Reads and displays it

Delta internally creates:

```
_delta_log/
```

This folder maintains version history.

---

## 2️⃣ Append + Time Travel

`DeltaAppendTimeTravel.java`

Steps:

1. Write initial data (Version 0)
2. Append new records (Version 1)
3. Read latest version
4. Read older version using:

```java
.option("versionAsOf", 0)
```

Example:

```
Reading Delta table latest:
1 Alice
2 Bob
3 Charlie
4 David
5 Emma

Reading Delta table version 0:
1 Alice
2 Bob
3 Charlie
```

---

## 3️⃣ Update + History + Time Travel

`DeltaUpdateTimeTravel.java`

Steps:

1. Initial write → Version 0
2. Update record (id = 2) → Version 1
3. Show table history
4. Read old and new versions

### Update Example

```java
deltaTable.update(
    col("id").equalTo(2),
    setMap
);
```

### Show History

```java
deltaTable.history().show(false);
```

### Time Travel

```java
.option("versionAsOf", 0)
.option("versionAsOf", 1)
```

---

# 🆚 Parquet vs Delta Lake

| Feature            | Parquet | Delta Lake                |
| ------------------ | ------- | ------------------------- |
| Storage Format     | Parquet | Parquet + Transaction Log |
| ACID Transactions  | ❌       | ✅                         |
| Schema Enforcement | Limited | Strong                    |
| Update/Delete      | ❌       | ✅                         |
| Time Travel        | ❌       | ✅                         |
| Table History      | ❌       | ✅                         |
| Streaming Support  | Basic   | Advanced                  |

---

# ⚙️ Maven Dependencies

From `pom.xml`:

```xml
<dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-sql_2.12</artifactId>
    <version>3.5.1</version>
</dependency>

<dependency>
    <groupId>io.delta</groupId>
    <artifactId>delta-spark_2.12</artifactId>
    <version>3.1.0</version>
</dependency>
```

---

# ▶ How To Run

### 1️⃣ Build

```
mvn clean package
```

### 2️⃣ Run Example

You can run from IDE
OR using:

```
mvn exec:java -Dexec.mainClass="com.oggu.spark.parqt.SparkWriteParquet"
```

Replace class name as needed.

---

# 🪟 Windows Notes

To avoid Spark binding errors:

```java
.config("spark.driver.bindAddress", "127.0.0.1")
.config("spark.driver.host", "127.0.0.1")
.config("spark.local.ip", "127.0.0.1")
.config("spark.hadoop.io.native.lib.available", "false")
```

These configurations are already included in the project.

---

# 📊 Output Locations

All data is written locally under:

```
output/
```

Example:

```
output/parquet_data
output/parquet_1M
output/delta-table
output/delta-update-table
```

---

# 🧠 Key Learnings

* How Spark writes distributed Parquet files
* Partition-based performance behavior
* How Delta Lake adds transactional layer over Parquet
* How versioning works internally via `_delta_log`
* How update operations create new versions
* How to query historical versions of data
* Practical implementation of ACID in Data Lake architecture

---

# 🏷 Suggested GitHub Topics

Add these tags to your repository:

```
apache-spark
delta-lake
big-data
java
parquet
data-engineering
spark-sql
time-travel
acid-transactions
data-lake
```

---

# 📄 License

This project is for educational and demonstration purposes.

You may add an MIT License if you want to make it reusable.

---

# 📌 Final Notes

This repository demonstrates the evolution from:

**Basic Parquet Storage → Transactional Delta Lake → Versioned Data Engineering**

It is designed as a practical reference implementation for:

* Data Engineers
* Spark Developers
* Big Data Learners
* Delta Lake Beginners

---

