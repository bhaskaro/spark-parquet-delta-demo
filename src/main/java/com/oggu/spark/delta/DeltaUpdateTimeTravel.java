package com.oggu.spark.delta;

import io.delta.tables.DeltaTable;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * Author : bhask
 * Created : 02-15-2026
 */
public class DeltaUpdateTimeTravel {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("DeltaUpdateExample")
                .master("local[*]")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .config("spark.driver.host", "127.0.0.1")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog",
                        "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .getOrCreate();

        String deltaPath = "output/delta-update-table";

        // ---------------------------
        // Step 1 — Initial Write
        // ---------------------------
        List<Person> data = Arrays.asList(
                new Person(1, "Alice"),
                new Person(2, "Bob"),
                new Person(3, "Charlie")
        );

        Dataset<Row> df = spark.createDataFrame(data, Person.class);

        df.write()
                .format("delta")
                .mode("overwrite")
                .save(deltaPath);

        System.out.println("Initial Delta table created.");

        // ---------------------------
        // Step 2 — UPDATE Operation
        // ---------------------------
        DeltaTable deltaTable = DeltaTable.forPath(spark, deltaPath);

        // Condition: id = 2
        Column condition = functions.col("id").equalTo(2);

        // Set clause
        Map<String, Column> setMap = new HashMap<>();
        setMap.put("name", functions.lit("Bob Updated"));

        deltaTable.update(condition, setMap);

        System.out.println("Update operation completed.");

        // ---------------------------
        // Step 3 — Read Latest Version
        // ---------------------------
        System.out.println("Latest Version:");
        deltaTable.toDF().show();

        // ---------------------------
        // Step 4 — Show History
        // ---------------------------
        System.out.println("Delta Table History:");
        deltaTable.history().show(false);

        // ---------------------------
        // Step 5 — Time Travel
        // ---------------------------
        System.out.println("Reading Version 0 (Before Update):");

        Dataset<Row> version0 = spark.read()
                .format("delta")
                .option("versionAsOf", 0)
                .load(deltaPath);

        version0.show();

        System.out.println("Reading Version 1 (After Update):");

        Dataset<Row> version1 = spark.read()
                .format("delta")
                .option("versionAsOf", 1)
                .load(deltaPath);

        version1.show();

        spark.stop();
    }
}
