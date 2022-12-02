//Description:

// we desired to check the effect on CPU using the openMPI by mimicing the pipe architecture.
// we designed in such a way that, the process scale without compiling

// headers used in the program
#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <math.h>

#define first_prime 2
#define MASTER_START_INT 1
int debug = 0;

// Signature for method which distributes the work
void divideTheWork(int*, int*,
                    unsigned long*,
                    unsigned int*, unsigned int*, unsigned int*,
                    unsigned int*, unsigned int*, unsigned int*);
// Main
// Takes the command arguments, if any, to determine when the program
// should stop finding primes and whether debug statements should be
// printed.  Each MPI processor then determines which set of integers
// that process is responsible for, allocates and initializes an array
// for that set of integers, and crosses off multiples for each prime
// number broadcasted by the master process.  The master process
// will always have all integers up to the sqrt(N) but can have more work
// if there are too few processes for a very large N.  The master process
// also crosses off multiples in its list and broadcasts the next uncrossed
// integer until it hits the last integer in the set.  Finally, at the end,
// through MPI_Reduce, all processes count how many primes they have in
// their array and report back to the master process.
// (input) int argc     main argument count
// (input) char** argv  main argument array
//                        argv[1] = n (largest int to check)
//                        argv[2] = "DEBUG" (to print debug statements)
// (output) int         return code
// return code 0        success
// return code 1        failed to allocate memory for one of the processes
int main(int argc, char** argv) {

   unsigned long n = 4294967295; // primes till 1 to N
   double timer; // variable to hold the time
   int p_count; // counter for the processess
   int p_id; // variable for storing the rank
   char processor_name[MPI_MAX_PROCESSOR_NAME]; // array to store the names of the processess currently execueting
   int name_len; // var to store the length of the name

   unsigned int n_master; // as described, we are considering the process 0 as master
   unsigned int n_worker; // the process after 1 are considered as
   unsigned int remainder; // variable to store the uneven work
   unsigned int p_work;  // counter variable for the set
   unsigned int p_first; // id indicating the first variable in the set
   unsigned int p_last; // id indicating the last variable in the set
   unsigned int prime; // var to store the current prime
   int j;
   int final_count; // counter to store the total number of primes found till now
   char *mark_table; // table for the process
    
    // initializing the environment
    MPI_Init(&argc, &argv);
    
    // computing the time for the execution
    MPI_Barrier(MPI_COMM_WORLD);
    timer = -MPI_Wtime(); // start timer

    // code for the communicator ( grouping holding and ranking the processess)
    MPI_Comm_size(MPI_COMM_WORLD, &p_count);
    MPI_Comm_rank(MPI_COMM_WORLD, &p_id);

    if(argc >= 2)
       n = strtoul(argv[1], NULL, 0);


    divideTheWork(&p_id, &p_count,
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


// Distribute Work
// This section of the code splits the workload, i.e. the total number of integers,
// as evenly as possible between all processes.  First, the master process is given
// all integers up to the sqrt(N).  Then, the rest of the work is divided among the
// other processes.  If the other processes still have more work than the master
// process, the work is re-divided back to give more work to master.  Remainders are
// evenly distributed among worker processes at the end.  Finally, the starting and
// ending integer in each set is determined.
//
// (input) int*          p_id           reference to this process' rank
// (input) int*          p_count        reference to num of processes in communicator
// (input) unsigned long* n             reference to total problem size
// (input) unsigned int* p_last         reference to this process' last integer
// (input) unsigned int* p_first        reference to this process' first integer
// (input) unsigned int* p_work         reference to this process' total int in set
// (input) unsigned int* n_master       reference to total work for master process
// (input) unsigned int* n_worker       reference to total work per worker process
// (input) unsigned int* remainder      reference to the remaining work, unevenly divided
// (return)   none      all references are changed directly
void divideTheWork(int *p_id,
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
