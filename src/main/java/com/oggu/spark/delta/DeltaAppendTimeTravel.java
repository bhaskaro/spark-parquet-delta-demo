package com.oggu.spark.delta;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

/**
 *
 * Author : bhask
 * Created : 02-15-2026
 */
public class DeltaAppendTimeTravel {

    public static void main(String[] args) {

        // Create Spark Session with Delta enabled
        SparkSession spark = SparkSession.builder()
                .appName("DeltaExample")
                .master("local[*]")

                // Windows driver fix
                .config("spark.driver.bindAddress", "127.0.0.1")
                .config("spark.driver.host", "127.0.0.1")

                // Required for Delta
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog",
                        "org.apache.spark.sql.delta.catalog.DeltaCatalog")

                .getOrCreate();


        // Sample data
        List<Person> data = Arrays.asList(
                new Person(1, "Alice"),
                new Person(2, "Bob"),
                new Person(3, "Charlie")
        );

        // Convert to DataFrame
        Dataset<Row> df = spark.createDataFrame(data, Person.class);

        // Write as Delta Table
        String deltaPath = "output/delta-table";

        //           Step 1 — Write initial data
        df.write()
                .format("delta")
                .mode("overwrite")
                .save(deltaPath);

        System.out.println("Delta table written successfully.");

        //           Step 2 — Append new data
        List<Person> newData = Arrays.asList(
                new Person(4, "David"),
                new Person(5, "Emma")
        );

        Dataset<Row> df2 = spark.createDataFrame(newData, Person.class);

        df2.write()
                .format("delta")
                .mode("append")
                .save(deltaPath);


        //        Step 3 — Read full table
        // Read Delta Table
        Dataset<Row> deltaTable = spark.read()
                .format("delta")
                .load(deltaPath);

        System.out.println("Reading Delta table latest:");
        deltaTable.show();

        //        Step 4 — Time Travel (Read Old Version)
        System.out.println("Reading Delta table version 0:");
        Dataset<Row> oldVersion = spark.read()
                .format("delta")
                .option("versionAsOf", 0)
                .load(deltaPath);

        oldVersion.show();

        spark.stop();
    }

}
