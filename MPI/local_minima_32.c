#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>

// Group number: 32
// Group members:
// Michele Conficconi
// Sara Ferrarese
// Jia Xing Chen

const int N = 256;

// Allocates and initializes matrix
int* generate_matrix() {
  int* A = (int*) malloc(N * N * sizeof(int));
  for (int i = 0; i < N * N; i++) {
    A[i] = rand() % 100;
  }
  return A;
}

// Returns the value at the given row and column
int val(int *A, int r, int c) {
  return A[r * N + c];
}

int main(int argc, char** argv) {
  MPI_Init(&argc, &argv);

  int rank, size;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &size);

  if (N % size != 0) {
    if (rank == 0) printf("N must be a multiple of the number of processes.\n");
    MPI_Finalize();
    return 0;
  }

  // ------------------------------------------------------------
  // Generate matrix on rank 0
  // ------------------------------------------------------------
  
  int* fullA = NULL;
  int* local = NULL;

  if (rank == 0) {
    fullA = generate_matrix();
  }

  // ------------------------------------------------------------
  // Distribute to all processes
  // ------------------------------------------------------------

  //for the inner pieces of the matrix, send to each process its part. Each inner process will receive (N/size)+2 rows, while the first and last process will receive (N/size)+1 rows
  // to allow overlapping borders, to minimize the communication afterwards, so each process can compute its local minima without further communication
  
  int local_rows;
  if (size == 1) {
    local_rows = N;               
  } else if (rank == 0 || rank == size - 1) {
    local_rows = (N / size) + 1;  /
  } else {
    local_rows = (N / size) + 2; 
  }

  local = (int*) malloc(local_rows * N * sizeof(int));

  if (rank == 0) {
    for (int j = 0; j < local_rows; j++) {
      for (int k = 0; k < N; k++) {
        local[j * N + k] = val(fullA, j, k);
      }
    }

    if (size > 1) {
      for (int p = 1; p < size - 1; p++) {
        MPI_Send(&fullA[(p * (N / size) - 1) * N], ((N / size) + 2) * N, MPI_INT, p, 0, MPI_COMM_WORLD);
      }
      MPI_Send(&fullA[((size - 1) * (N / size) - 1) * N], ((N / size) + 1) * N, MPI_INT, size - 1, 0, MPI_COMM_WORLD);
    }
  } else {
    MPI_Recv(local, local_rows * N, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
  }

  // ------------------------------------------------------------
  // Free global matrix
  // ------------------------------------------------------------

  if(rank == 0){
    free(fullA);
  }

  // ------------------------------------------------------------
  // Exchange futher information if needed
  // ------------------------------------------------------------

  // If we used the scatter to exchange the parts of the matrix, we would need to exchange the borders now
  // However, since we already sent the overlapping borders, no further communication is needed
  
  // ------------------------------------------------------------
  // Compute local minima (excluding GLOBAL borders)
  // ------------------------------------------------------------

  int local_min; 
  int end_row = local_rows; 
  
  int *minima_positions = (int*) calloc((N / size), sizeof(int)); 

  for (int i = 1; i < (end_row - 1); i++) { 
    for (int j = 1; j < N - 1; j++) {      
      local_min = val(local, i, j);
      if (val(local, i - 1, j) <= local_min) continue;
      if (val(local, i + 1, j) <= local_min) continue;
      if (val(local, i, j - 1) <= local_min) continue;
      if (val(local, i, j + 1) <= local_min) continue;

      // If we reach here, (i, j) is a local minimum
      if (rank == 0) {
        minima_positions[i]++; 
      } else {
        minima_positions[i - 1]++; 
      }
    }
  }

  // ------------------------------------------------------------
  // Send results to rank 0 and print results on rank 0
  // ------------------------------------------------------------

  int * tot_min = NULL;

  if(rank == 0){
    tot_min = (int*) malloc(N * sizeof(int));
  }

  MPI_Gather(minima_positions, N / size, MPI_INT, tot_min, N / size, MPI_INT, 0, MPI_COMM_WORLD);

  if(rank == 0){
    for(int i = 0; i < N; i++){
      printf("– Row: %d, local minima: %d \n", i, tot_min[i]); 
    }
    free(tot_min);
  }

  // ------------------------------------------------------------
  // Free allocated memory
  // ------------------------------------------------------------

  free(local);
  free(minima_positions);


  MPI_Finalize();
  return 0;
}
