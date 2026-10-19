# Apache Spark Evaluation Lab
**Networked Software for Distributed Systems**

This module implements a batch and stream data processing pipeline using the **Apache Spark SQL and Structured Streaming** Java APIs. The application analyzes static historical purchase data alongside real-time streaming purchase events to extract insights and calculate rolling metrics for top-selling products.

## Data Sources

The application ingests three distinct datasets:
*   **Products (Static):** A CSV file (`products.csv`) containing product catalogs with fields for `ProductID`, `Category`, and `Price`.
*   **Historical Purchases (Static):** A CSV file (`historicalPurchases.csv`) containing historical records of `ProductID` and the `Quantity` purchased.
*   **Purchases Stream (Streaming):** A simulated real-time data stream (using the `rate` format) generating 5 rows per second, mapping a counter to specific product IDs and randomized purchase quantities (1-5 units) alongside a `timestamp`.

## Query Implementations

The evaluation lab requires executing four specific queries, progressively building upon the results of the previous ones:

*   **Q1 (Top Products):** Computes the top 5 most purchased products by total quantity using the historical data. 
*   **Q2 (Revenue Analysis):** Computes the total revenue for each of the top 5 products. It joins the result of Q1 with the `products` dataset and calculates the revenue by multiplying `Price` and `totalQuantity`, outputting the `Category`, `ProductID`, and `totalRevenue`.
*   **Q3 (Streaming Window Aggregation):** Computes the rolling sum of purchased units for the top 5 products identified in Q1. It performs a streaming join between the top products and the real-time `purchasesStream`, grouping the data into a sliding time window (15 seconds size, 5 seconds slide) and aggregating the `Quantity`. Results are outputted to the console in `update` mode.
*   **Q4 (Historical vs. Streaming Difference):** Compares the historical volume against real-time trends. Using the windowed streaming dataframe from Q3, it calculates the mathematical difference between the historical `totalQuantity` and the streaming window's `streamQuantity`. Results are outputted to the console in `update` mode.

## Optimizations & Design Choices

To adhere to the requirement of limiting unnecessary recomputations, the following optimizations were implemented:
*   **Caching:** The `topProducts` dataset computed in Q1 is explicitly cached (`topProducts.cache()`) in memory. Since Q2, Q3, and Q4 all rely on these top 5 products, caching prevents Spark from re-evaluating the initial sorting and limiting operations for every downstream action.
*   **GroupBy Payload Carry-over:** In Q3, `totalQuantity` is intentionally included as an attribute in the `groupBy` clause alongside the window and `ProductID`. Although it doesn't provide further grouping granularity, this technique forces Spark to carry the column through the aggregation. This cleverly avoids the need to perform an expensive re-join between the streaming aggregate and the static `topProducts` dataset to fetch the `totalQuantity` for Q4's difference calculation.

## Group 32
*   Conficconi Michele
*   Ferrarese Sara
*   Chen Jia Xing