package it.polimi.spark.eval;

/*
 * Group 32
 * Members:
 * Conficconi Michele
 * Ferrarese Sara
 * Chen Jia Xing
 */

import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.*;
import static org.apache.spark.sql.functions.*;

public class ProductAnalytics32 {

    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession.builder()
                .master("local[8]")
                .appName("ProductAnalytics")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        // Load static datasets
        Dataset<Row> products = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("input/products.csv");
        // ProductID, Category, Price

        Dataset<Row> historicalPurchases = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("input/historicalPurchases.csv");
        // ProductID, Quantity

        // Streaming source: purchasesStream
        Dataset<Row> purchasesStream = spark.readStream()
                .format("rate")
                .option("rowsPerSecond", 5)
                .load();

        // Map the "value" counter to product IDs and quantities
        purchasesStream = purchasesStream
                .select(
                        col("timestamp"),
                        when(col("value").mod(10).lt(2), lit("P0582"))
                                .when(col("value").mod(10).lt(3), lit("P0112"))
                                .when(col("value").mod(10).lt(5), lit("P0536"))
                                .when(col("value").mod(10).equalTo(6), lit("P0742"))
                                .when(col("value").mod(10).equalTo(8), lit("P0027"))
                                .otherwise(lit("P00" + Math.random() % 100))
                                .alias("Product"),
                        (col("value").mod(5).plus(1)).alias("Quantity")       // 1–5 units
                );

        // ==========================================
        // Q1: Top 5 most purchased products
        // ==========================================

        Dataset<Row> topProducts = historicalPurchases
                .sort(desc("Quantity"))
                .withColumnRenamed("Quantity", "totalQuantity")
                .limit(5);

        topProducts.cache();

        topProducts.show();


        // ==========================================
        // Q2: Revenue per (Category, TopProduct)
        // ==========================================

        Dataset<Row> revenue = topProducts
                .join(products, topProducts.col("ProductID").equalTo(products.col("ProductID")), "left")
                .withColumn("totalRevenue", col("Price").multiply(col("totalQuantity")))
                .select(col("Category"), topProducts.col("ProductID"), col("totalRevenue"));

        revenue.show();

        // ==========================================
        // Q3: Streaming window for top products
        // ==========================================

        // we added the attribute "topProducts.col("totalQuantity")" to the groupBy method, even though it doesn't provide further grouping,
        // to prevent the dropping of the "totalQuantity" attribute after the groupBy. In this way we can avoid recomputing a join in the diff query.
        // We are aware that technically the groupBy shouldn't drop the column, but we found similar feedback even on stackoverflow
        final Dataset<Row> windowed = topProducts
                .join(purchasesStream, topProducts.col("ProductID").equalTo(purchasesStream.col("Product")))
                .groupBy(window(col("timestamp"), "15 seconds", "5 seconds"),
                        topProducts.col("ProductID"), topProducts.col("totalQuantity"))
                .sum("Quantity")
                .withColumnRenamed("sum(Quantity)", "streamQuantity")
                .withColumnRenamed("ProductID", "Product");


        final StreamingQuery q3 = windowed
                .drop("totalQuantity")
                .writeStream()
                .outputMode("update")
                .format("console")
                .start();

        // ==========================================
        // Q4: Difference historical vs streaming
        // ==========================================

        final StreamingQuery diff = windowed
                .withColumn("difference", topProducts.col("totalQuantity").minus(col("streamQuantity")))
                .select("window", "Product", "totalQuantity", "streamQuantity", "difference")
                .writeStream()
                .outputMode("update")
                .format("console")
                .start();


        try {
            q3.awaitTermination();
            diff.awaitTermination();
        } catch (final StreamingQueryException e) {
            e.printStackTrace();
        }
    }
}
