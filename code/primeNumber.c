#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <math.h>

#define first_prime 2 // First prime number
#define MASTER_START_INT 1 // First integer in the master process' int set
int debug = 0; // Trigger for debug statements

// Signature for method which distributes the work
void divideTheWork(int*, int*,
                    unsigned long*,
                    unsigned int*, unsigned int*, unsigned int*,
                    unsigned int*, unsigned int*, unsigned int*);

int main(int argc, char** argv) {

   unsigned long n = 4294967295; // Find all primes up to n
   double timer; // Execution time
   int p_count; // Number of processes
   int p_id; // Get process rank
   char processor_name[MPI_MAX_PROCESSOR_NAME]; // Stores process name
   int name_len; // Stores process name length

   unsigned int n_master; // Work for process 0
   unsigned int n_worker; // Work for process 1+
   unsigned int remainder; // Uneven remaining work
   unsigned int p_work;  // Count of intgers in set
   unsigned int p_first; // First integer in set
   unsigned int p_last; // Last integer in set
   unsigned int prime; // Current prime to remove multiples
   int j; // General iterator;
   int final_count; // Total number of primes found
   char *mark_table; // Each process' integer marking table

    MPI_Init(&argc, &argv); // Initialize MPI Environment
  ////===================================================

  //// Start Timer
    MPI_Barrier(MPI_COMM_WORLD);
    timer = -MPI_Wtime(); // start timer

  //// Perform MPI Housekeeping
    MPI_Comm_size(MPI_COMM_WORLD, &p_count);  // Get the number of processes
    MPI_Comm_rank(MPI_COMM_WORLD, &p_id);  // Get the rank of the process

  //// Check input arguments
    if(argc >= 2) // Change upper range of domain
       n = strtoul(argv[1], NULL, 0);
    if(argc == 3 && (argv[2] == "DEBUG") ) // Turn on debug mode
    {
       debug = 1;
    }

 //// Determine work distributed to this process
    distributeWork(&p_id, &p_count,
                   &n,
                   &p_last, &p_first, &p_work,
                   &n_master, &n_worker, &remainder);

  //// Print each process' domain (if debug)
    if(debug)
    {
       printf("Proc %d: Assigned %d (%d - %d)\n", p_id, p_work, p_first, p_last);
       MPI_Barrier(MPI_COMM_WORLD);
    }


  //// Allocate array for each process
    mark_table = (char*) malloc( sizeof(char)*p_work );
    // Check if successful; if not, end program
    if(mark_table == NULL)
    {
       printf("Failed to allocate memory for process %d\n", p_id);
       MPI_Finalize();
       exit(1);
    }
    // Initialize the arrays
    for(j = 0; j < p_work; j++)
       mark_table[j] = '0'; // Designate '0' as unmarked and '1' as marked

    if(!p_id) mark_table[0] = '1';

  //// Find primes and sieve multiples
    int marker; // Each process' marker (current multiple)
    prime = first_prime; // Broadcasted prime for sieving multiples
    do
    {
        // Calculate first multiple to cross off
       if(prime < p_first) // If current prime less than first int in set
       {
          int mod = p_first % prime; // Take modulus
          if(mod) // If not divisible...
             marker = prime - mod;  // Set to index of next multiple, assuming in set
             // Note: index = p_first - mod + i - p_first
          else // If divisible...
             marker = 0;  // Set first multiple to index 0 (first integer in set)
       }
       else // If current prime greater than first int in set
       {
          marker = 2*prime - p_first; // Set to index of that prime's next multiple, assuming in set
       }

       // While the current index is in range, mark off multiples
       for(j = marker; j < p_work; j += prime)
       {
          mark_table[j] = '1'; // 1s designate multiples (non-primes)
       }

       // If process 0 (master process), broadcast the next prime
       if(!p_id)
       {
          int next_index = prime - MASTER_START_INT; // Set to current index
          do{
             if(++next_index >= n_master) // Get the next index
             { // However, if greater than or equal to  last int in master set
                next_index == n_master - MASTER_START_INT; // Set equal to max and break
                break;
             }
          }while(mark_table[next_index] != '0'); // Keep going till found prime
          prime = next_index + MASTER_START_INT; // Convert index to prime number
       }

       // Broadcast the next prime from process 0
       MPI_Bcast(&prime, 1, MPI_INT, 0, MPI_COMM_WORLD);

     // Keep going till the broadcasted int is the last int in master set
    }while(prime <= n_master );

  //// Print Section for DEBUG MODE
    // Prints each process' primes
    // (NOTE: Due to buffer and connection, print statements may not be clean)
    if(debug)
    {
       // All processes besides proc 0 must wait for OK to proceed
       if(p_id)
       {  // Blocking MPI_Recv to wait for the previous process to signal.
          MPI_Recv(&prime, 1, MPI_INT, (p_id-1), 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
       }

       // Print all primes in this process
       printf("Proc %d: ", p_id);
       for(j = 0; j < p_work; j++)
       {
          if(mark_table[j] == '0')
             printf("%d, ", j + p_first);
       }
       printf("\n");
       fflush(stdout);

       // If not the last process in this communicator
       if(p_id != (p_count - 1))
       {
          // Signal the next process to proceed with print statement
          MPI_Send(&prime, 1, MPI_INT, (p_id+1), 1, MPI_COMM_WORLD);
          // I'm done!
       }
    }

  //// Gather the total of primes found
    prime = 0;
    // Each process uses parallel threads to help with this step
    #pragma omp parallel for reduction(+:prime)
    for(j = 0; j < p_work; j++)
    {
       if(mark_table[j] == '0')
          prime++;
    }
    MPI_Reduce(&prime, &final_count, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);

  //// End Timer
    MPI_Barrier(MPI_COMM_WORLD);
    timer += MPI_Wtime();

  //// Master process prints final result
    if(!p_id)
    {
     if(debug) printf("Count of primes up to %d: %d\n", n, final_count);
     printf("Total elapsed time: %10.6f\n", timer);
    }

    //===================================================
    MPI_Finalize();  // Finalize the MPI environment.
}

void distributeWork(int *p_id,
                    int *p_count,
                    unsigned long *n,
                    unsigned int *p_last,
                    unsigned int *p_first,
                    unsigned int *p_work,
                    unsigned int *n_master,
                    unsigned int *n_worker,
                    unsigned int *remainder)
{
    // First, divide work assuming master process only needs sqrt(n) work
    *n_master = (unsigned int) ceil(sqrt((double) *n)); // Give master process all base primes
    *n_worker = (*n - *n_master)/(*p_count - 1); // Distribute evenly remaining work
    *remainder = (*n - *n_master)%(*p_count - 1); // Find remaining uneven work

    // If workers have more than master thread, redistribute
    if(*n_worker > *n_master)
    {
       *n_master = *n_worker = *n / *p_count; // All processes get even work
       *remainder = *n % *p_count; // Remainder to be divided
    }

    // Now, figure out ever process' first and last integer
    // Also, distribute remainder evenly among workers
    if(!*p_id) // Master process
    {
       *p_last = *p_work = *n_master; // Master process' work
       *p_first = MASTER_START_INT;
    }
    else // Worker processes
    {
       *p_work = *n_worker; // Worker process' work

       if(*p_id <= *remainder) // Additional piece distributed for remainder
          (*p_work)++;

       *p_last = *n_master + (*n_worker * *p_id); // Last int

       if(*p_id <= *remainder) // Correction for remainder
          *p_last += *p_id;
       else
          *p_last += *remainder;
       // Calculate first int based on last int and work given
       *p_first = *p_last - *p_work + 1;
    }
}
