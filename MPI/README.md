# MPI Evaluation Lab
**Networked Software for Distributed Systems**

This module implements a parallel MPI (Message Passing Interface) program in C designed to find all local minima in a randomly generated 2-dimensional matrix (256x256).

## System Architecture & Data Distribution

The computation is distributed across multiple MPI processes to ensure efficient parallel execution. 
* **Initialization:** Process 0 is responsible for allocating and initializing the entire 256x256 matrix in a linear array using row-major format.
* **Overlapping Distribution (Halo Rows):** To minimize communication overhead, the implementation avoids the standard `MPI_Scatter` followed by secondary border exchanges. Instead, Process 0 directly distributes overlapping sub-matrices using `MPI_Send`. 
  * The top (Rank 0) and bottom (Rank size-1) processes receive their base fraction of rows plus 1 overlapping row.
  * All intermediate processes receive their base fraction of rows plus 2 overlapping rows (one for the top border, one for the bottom border).

This strategic data partitioning allows every process to compute its local minima entirely independently without needing further network communication to check edge neighbors.

## Processing Logic

A point is considered a "local minimum" if its value is strictly less than all four of its adjacent neighbors (top, bottom, left, and right). 
* By the assignment's definition, boundary points of the global matrix are excluded, so the algorithm only evaluates internal points.
* Each process iterates through its assigned internal rows and columns, checking the neighbor condition and incrementing a local counter array that tracks the number of minima found on each specific row.

## Result Aggregation

Once the parallel computation concludes, the processes synchronize their findings:
* The system utilizes `MPI_Gather` to collect the local arrays of row-by-row minima counts from all worker processes.
* Process 0 receives this gathered data into a single, ordered master array and prints the total number of local minima found per row to the standard output.
* Memory for both the local sub-matrices and the global matrix is dynamically allocated during setup and correctly freed before calling `MPI_Finalize`.

## Group 32
* Michele Conficconi
* Sara Ferrarese
* Jia Xing Chen