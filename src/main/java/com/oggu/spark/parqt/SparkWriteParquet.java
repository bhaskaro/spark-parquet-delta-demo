package com.oggu.spark.parqt;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

/**
 *
 * Author : bhask
 * Created : 02-11-2026
 */
public class SparkWriteParquet {

    public static void main(String[] args) {

//        SparkSession spark = SparkSession.builder()
//                .appName("Spark Parquet Write Example")
//                .master("local[*]")   // run locally
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


        // Sample data
        List<Row> data = Arrays.asList(
                org.apache.spark.sql.RowFactory.create(1, "Laptop", "Electronics", 1200),
                org.apache.spark.sql.RowFactory.create(2, "Mouse", "Electronics", 50),
                org.apache.spark.sql.RowFactory.create(3, "Desk", "Furniture", 450)
        );

        StructType schema = new org.apache.spark.sql.types.StructType()
                .add("order_id", "int")
                .add("product", "string")
                .add("category", "string")
                .add("price", "int");

        Dataset<Row> df = spark.createDataFrame(data, schema);

        df.show();

        df.write()
                .mode("overwrite")
                .parquet("output/parquet_data");

        System.out.println("Parquet written successfully!");

        spark.stop();
    }
}
