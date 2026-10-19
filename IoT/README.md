# Contiki-NG IoT Evaluation Lab
**Networked Software for Distributed Systems**

This module implements a networked data structure over an IPv6 RPL tree using the Contiki-NG operating system. The system allows distributed modifications through a lock-based concurrency mechanism over UDP, ensuring that only one node can modify the data structure at a time.

## Protocol & Message Structure

To ensure deterministic memory alignment and reliable packet handling, communication relies on a fixed-size C structure (`msg_t`) rather than parsing variable-length string buffers. The protocol utilizes explicit operation codes:
*   `OP_LOCK (1)`: Client requests an exclusive lock.
*   `OP_READ (2)`: Client requests the current value.
*   `OP_WRITE (3)`: Client attempts to overwrite the data structure.
*   `OP_READ_REPLY (4)`: Server returns the current data structure value.
*   `OP_WRITE_ERROR (5)`: Server rejects an unauthorized write.

## Locking Mechanism & Concurrency

The server (acting as the DAG root) maintains the state of the data structure and manages client access.
*   **Acquisition:** When a lock is granted, the server stores the client's IPv6 address (`lock_ip`) and initiates a 5-second `ctimer`. 
*   **Validation:** Write operations are strictly accepted only if the sender's IP address matches the stored `lock_ip` and the lock has not expired.
*   **Release:** The lock is automatically released either immediately after a successful write operation or when the 5-second timer expires.

## Deterministic Test Scenario

The two clients (`udp-client_1` and `udp-client_2`) utilize static variables to preserve their state across Protothread yields. They execute a coordinated, deterministic timeline to validate all edge cases:

1.  **Standard Write:** Client 1 locks the structure, preventing Client 2 from acquiring it. Client 1 writes `2` and automatically unlocks it.
2.  **Unauthorized Access:** Client 2 reads the value (`2`), then attempts to write `3` without holding a lock. The server rejects the operation and returns `OP_WRITE_ERROR`.
3.  **Timeout Expiration:** Client 1 acquires the lock again and reads the value, but pauses until the server's 5-second timer expires. When Client 1 finally attempts to write `4`, the server correctly identifies the expired lock and rejects it.
4.  **Final Resolution:** Client 2 successfully acquires the lock, writes `5`, and both clients independently read the final structure to verify data consistency.

## Group 32
*   Conficconi Michele
*   Ferrarese Sara
*   Chen Jia Xing