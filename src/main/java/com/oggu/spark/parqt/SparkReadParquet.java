package com.oggu.spark.parqt;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 *
 * Author : bhask
 * Created : 02-11-2026
 */
public class SparkReadParquet {

    public static void main(String[] args) {

//        SparkSession spark = SparkSession.builder()
//                .appName("Spark Parquet Read Example")
//                .master("local[*]")
//                .getOrCreate();

        SparkSession spark = SparkSession.builder()
                .appName("Spark Parquet Write Example")
                .master("local[*]")

                // Fix driver binding issue
                .config("spark.driver.bindAddress", "127.0.0.1")
                .config("spark.driver.host", "127.0.0.1")

                // Optional safety
                .config("spark.hadoop.io.native.lib.available", "false")

                .getOrCreate();

        Dataset<Row> df = spark.read()
                .parquet("output/parquet_data");

        df.show();

        df.printSchema();

        spark.stop();
    }
}

