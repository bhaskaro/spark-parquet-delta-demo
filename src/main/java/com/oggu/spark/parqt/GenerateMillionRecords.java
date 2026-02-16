package com.oggu.spark.parqt;



import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

/**
 *
 * Author : bhask
 * Created : 02-11-2026
 */
public class GenerateMillionRecords {

    public static void main(String[] args) throws Exception {

        SparkSession spark = SparkSession.builder()
                .appName("Generate 1M Parquet")
                .master("local[*]")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .config("spark.driver.host", "127.0.0.1")
                .config("spark.local.ip", "127.0.0.1")
                .config("spark.hadoop.io.native.lib.available", "false")
                .getOrCreate();

        long recordCount = 1_000_000;

        // Generate distributed dataset
        Dataset<Row> df = spark.range(recordCount)
                .withColumnRenamed("id", "order_id")
                .withColumn("product",
                        when(col("order_id").mod(3).equalTo(0), "Laptop")
                                .when(col("order_id").mod(3).equalTo(1), "Mouse")
                                .otherwise("Desk"))
                .withColumn("category",
                        when(col("product").equalTo("Laptop"), "Electronics")
                                .when(col("product").equalTo("Mouse"), "Electronics")
                                .otherwise("Furniture"))
                .withColumn("price",
                        when(col("product").equalTo("Laptop"), 1200)
                                .when(col("product").equalTo("Mouse"), 50)
                                .otherwise(450));

        System.out.println("Partitions: " + df.rdd().getNumPartitions());

        df.printSchema();

        // Measure write time
        long start = System.currentTimeMillis();

        df.write()
                .mode("overwrite")
                .option("compression", "snappy")
                .parquet("output/parquet_1M");

        long end = System.currentTimeMillis();

        double seconds = (end - start) / 1000.0;
        System.out.println("Write time (seconds): " + seconds);

        spark.stop();
    }
}
