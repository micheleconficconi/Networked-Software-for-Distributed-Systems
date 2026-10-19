# Akka Evaluation Lab
**Networked Software for Distributed Systems**

This module implements a highly available, fault-tolerant replicated address book using the **Actor Model** in Java with the Akka framework. The system is designed to simulate distributed data partitioning, redundant storage, and graceful degradation during simulated node failures using timeouts and the Ask pattern.

## System Architecture

The topology consists of four primary actors communicating asynchronously:

*   **Client Actor:** Acts as the gateway for user operations. It issues write (`PutMsg`) and read (`GetMsg`) requests to the system and handles the final responses, including timeout exceptions.
*   **Balancer Actor:** The core orchestrator and router. It encapsulates the replication logic, partitions incoming data across the worker nodes, and manages fault-tolerant query resolution.
*   **Worker Actors (Worker 0 & Worker 1):** The distributed state stores. Each worker maintains internal memory for both primary and secondary data replicas. Their behavior can be toggled to simulate node failures.

## Data Partitioning & Replication Logic

To ensure high availability, every address book entry is replicated across both workers, but prioritized based on a deterministic partitioning key: the initial letter of the contact's name.

1.  **Write Operations:** When the Client dispatches a `PutMsg` (containing a Name and Email), the Balancer intercepts it.
2.  **Splitting:** The Balancer determines the primary worker based on the name's initial (e.g., A-M targets Worker 0 as primary, N-Z targets Worker 1 as primary). 
3.  **Replication:** The Balancer generates a `PutSplitMsg` and sends it to *both* workers. The message explicitly flags whether the worker should store the entry in its primary (`1`) or secondary (`2`) storage tier.

## Query Resolution & Fault Tolerance

The system leverages Akka's **Ask Pattern** to guarantee reliable query resolution even when individual worker nodes drop offline.

*   **Standard Retrieval:** When the Client issues a `GetMsg`, the Balancer first queries the designated *primary* worker for that specific record. If the primary worker is healthy, it returns the email via a `ReplyMsg`, which is passed back to the Client.
*   **Simulated Node Failure (`RestMsg`):** Workers can receive a `RestMsg`, which alters their internal behavior to silently drop incoming queries, simulating a crashed node or network partition. (They can be restored later via a `ResumeMsg`).
*   **Secondary Fallback:** If the primary worker is resting, the Balancer's ask timeout will expire. Instead of failing the operation, the Balancer automatically falls back and queries the *secondary* worker holding the replica.
*   **Total Outage:** If both the primary and secondary workers are unresponsive, the Balancer's secondary timeout expires. It then generates a `TimeoutMsg` and returns it to the Client, which throws a defined exception logging that the cluster is unavailable.

## Execution Flow

The entire lifecycle is demonstrated in `ReplicatedAddressBook.java`:

1.  **Initialization:** `SetupClientMsg` injects the Balancer's reference into the Client, and `SetupMsg` injects the Workers' references into the Balancer.
2.  **Population:** The Client adds entries (e.g., "Alessandro", "Salvatore").
3.  **Primary Query:** The Client successfully retrieves "Alessandro" from Worker 0.
4.  **Graceful Degradation:** Worker 0 receives a `RestMsg`. The Client queries "Alessandro" again; the Balancer hits a timeout on Worker 0 and successfully retrieves the replica from Worker 1.
5.  **System Failure:** Worker 1 also receives a `RestMsg`. The subsequent query for "Alessandro" results in a system-wide `TimeoutMsg` returned to the Client. 

## Group 32
*   Michele Conficconi
*   Sara Ferrarese
*   Jia Xing Chen