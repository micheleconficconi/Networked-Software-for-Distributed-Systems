# Networked Software for Distributed Systems (NSDS)

This repository contains a collection of projects and evaluation labs developed for the Networked Software for Distributed Systems course at Politecnico di Milano. The repository is divided into six modules, each exploring a different paradigm of distributed computing, ranging from actor-based models and stream processing to low-power IoT networks and high-performance parallel computing.

## Tech Stack & Paradigms

The projects are organized based on the core technologies and communication patterns used during the course:

### 1. Actor Model & Reactive Systems (`/Akka`)
*   **Technology:** Java, Akka (Untyped)
*   **Concepts:** Location transparency, asynchronous message passing, data partitioning, and fault tolerance.
*   **Focus:** Implementation of a highly available, replicated address book. Features custom load-balancing, primary/secondary replica routing, and graceful degradation using timeouts and the Ask pattern to handle simulated node failures.

### 2. Message Broking & Event Streaming (`/Apache Kafka`)
*   **Technology:** Java, Apache Kafka
*   **Concepts:** Publish-subscribe pattern, consumer groups, stateful aggregation, and transactional processing.
*   **Focus:** Building a fault-tolerant stream processing topology. Includes a stateful `Merger` ensuring at-least-once semantics, and a `Validator` utilizing Kafka Transactions for atomic, exactly-once delivery across multiple downstream topics.

### 3. Big Data & Stream Processing (`/Apache Spark`)
*   **Technology:** Java, Apache Spark (SQL & Structured Streaming)
*   **Concepts:** Batch processing, streaming DataFrames, stream-static joins, and sliding time windows.
*   **Focus:** Processing large-scale static historical data alongside real-time purchase streams. The pipeline computes top-selling products, calculates revenue, and performs rolling window aggregations while limiting computational overhead via caching.

### 4. Low-Power IoT & Wireless Sensor Networks (`/IoT`)
*   **Technology:** C, Contiki-NG, Cooja Simulator
*   **Concepts:** IPv6, RPL routing, UDP communication, protothreads, and distributed concurrency.
*   **Focus:** Programming resource-constrained devices to manage a centralized, networked data structure over a DAG root. Implements a distributed locking mechanism to ensure safe, concurrent write operations from multiple UDP clients.

### 5. High-Performance Computing (`/MPI`)
*   **Technology:** C, MPI (Message Passing Interface)
*   **Concepts:** Parallel algorithms, domain decomposition, overlapping borders (halo rows), and collective communications (`MPI_Gather`).
*   **Focus:** Optimizing the computation of local minima in a 2D matrix. Data is distributed with overlapping boundaries to allow independent parallel processing across memory clusters without mid-computation network overhead.

### 6. Low-Code Integration & Flow-Based Programming (`/NodeRed`)
*   **Technology:** Node-RED, JavaScript
*   **Concepts:** Event-driven architecture, visual wiring of APIs, and state management.
*   **Focus:** Rapid prototyping of a Telegram bot that interfaces dynamically with the OpenWeatherMap API. The flow parses multi-word queries, retrieves real-time weather data, and maintains independent tracking lists for individual users.