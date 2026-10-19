# Apache Kafka Evaluation Lab
**Networked Software for Distributed Systems**

This module implements a stream processing topology using the **Apache Kafka** Java API. The system consists of producing sensors, a stateful aggregation component, and a transactional validation component that ensures atomic delivery to downstream topics.

## System Architecture

The topology is divided into three main stages of data flow:

*   **Sensors (Producer):** Simulates IoT data generation. It continuously produces randomized integer values with associated string keys (e.g., "Key1") to two distinct input topics: `sensors1` and `sensors2`.
*   **Merger Component:** A stateful consumer-producer application that reads from both `sensors1` and `sensors2`. It maintains an internal memory (via HashMaps) of the latest value received for each key from each topic. For every new message, it computes the sum of the latest values from both sensors and publishes the aggregated result to the `merged` topic. If a key has only appeared on one topic, it outputs that single value.
*   **Validator Component:** A transactional consumer-producer that reads the aggregated values from the `merged` topic and forwards them to two downstream topics: `output1` and `output2`.

## Semantics and Guarantees

*   **At-Least-Once Delivery (Merger):** The Merger disables automatic offset committing (`ENABLE_AUTO_COMMIT_CONFIG = false`) and relies on manual synchronous commits (`commitSync()`) at the end of each polling batch. This guarantees that no messages are lost in the event of a crash, fulfilling the at-least-once semantic requirement.
*   **Transactional Atomicity (Validator):** To ensure that downstream systems either receive the message on *both* output topics or *neither*, the Validator utilizes Kafka Transactions. It initializes a transaction (`beginTransaction()`), sends the records to `output1` and `output2`, links the consumer offsets to the transaction (`sendOffsetsToTransaction()`), and commits it atomically (`commitTransaction()`). It uses a specific `TRANSACTIONAL_ID_CONFIG` ("validatorId") and enables idempotence.

## Scalability and Partitioning

*   **Partition Alignment:** To allow multiple instances of the Merger to run concurrently within the same consumer group, the number of partitions for `sensors1` and `sensors2` must be identical. This ensures that messages with the same key from both topics are deterministically routed to the same Merger instance.
*   **Validator Scaling:** Multiple instances of the Validator can operate in the same consumer group to process multiple partitions of the `merged` topic in parallel without violating the atomic commitment constraints.

## Group 32
*   Conficconi Michele
*   Ferrarese Sara
*   Chen Jia Xing