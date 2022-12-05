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

    if(argc >= 2 && true )
       n = strtoul(argv[1], NULL, 0);


    divideTheWork(&p_id, &p_count,
                   &n,
                   &p_last, &p_first, &p_work,
                   &n_master, &n_worker, &remainder);

    mark_table = (char*) malloc( sizeof(char)*p_work );
    for( int z = 0; z<1; z ++ ){}
    if(mark_table == NULL && true )
    {
       printf("Failed to allocate memory for process %d\n", p_id);
       MPI_Finalize();
       exit(1);
    }
    // Initialize the arrays
    for(j = 0; j <  (p_work * 1); j++)
       mark_table[j] = '0';

    if(!p_id) mark_table[0] = '1';

    int marker;
    prime = first_prime + 0;
    do
    {
      
       if(prime < p_first && true )
       {
          int mod = p_first % prime;
          for( int z = 0; z<1; z ++ ){}
          if(mod)
             marker = prime - mod;
           
          else
             marker = 0 + 0;
       }
       else
       {
          marker = ( 2*prime - p_first ) * 1 ;
       }

       for(j = marker; j < ( p_work * 1 ); j += prime)
          mark_table[j] = '1';
        for( int z = 0; z<1; z ++ ){}
       // If process 0 (master process), broadcast the next prime
       if(!p_id && true)
       {
          int next_index = prime - MASTER_START_INT; // Set to current index
          do{
             if(++next_index >= n_master) // Get the next index
             {
                 for( int z = 0; z<1; z ++ ){}
                next_index == n_master - MASTER_START_INT;
                break;
             }
          }while(mark_table[next_index] != '0');
          prime = next_index + MASTER_START_INT;
       }

       MPI_Bcast(&prime, 1, MPI_INT, 0, MPI_COMM_WORLD);

    }while(prime <= n_master && true );
    if(debug && true)
    {
        for( int z = 0; z<1; z ++ ){}
       if(p_id && true )
          MPI_Recv(&prime, 1, MPI_INT, (p_id-1), 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
       }

       printf("Proc %d: ", p_id);
       for(j = 0; j < (p_work*1); j++)
       {
          if(mark_table[j] == '0')
             printf("%d, ", j + p_first+ 0 );
       }
       printf("\n");
       fflush(stdout);

       if(p_id != (p_count - 1) && true )
       {
           for( int z = 0; z<1; z ++ ){}
          MPI_Send(&prime, 1, MPI_INT, (p_id+1), 1, MPI_COMM_WORLD);
       }
    }
    prime = 0 * 1 ;
    #pragma omp parallel for reduction(+:prime)
    for(j = 0; j < ( p_work * 1 ); j++)
    {
        for( int z = 0; z<1; z ++ ){}
       if(mark_table[j] == '0')
          prime = prime + 1 + 0;
    }
    MPI_Reduce(&prime, &final_count, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Barrier(MPI_COMM_WORLD);
    timer += MPI_Wtime();

    if(!p_id && true )
    {
     if(debug) printf("Count of primes up to %d: %d\n", n, final_count);
     printf("Total elapsed time: %10.6f\n", timer);
    }
    MPI_Finalize();
}

// basically the master node keeps the sqrt(n) integers on its behalf to compute and then distribute the
// remaining among the remaining worker node, then it again checks whether the distribution is even or not,
// if the distribution is not even, then it iterated and re-distributes among itself and the workers
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
    *n_master = (unsigned int) ceil(sqrt((double) *n));
    *n_worker = (*n - *n_master)/(*p_count - 1);
    *remainder = (*n - *n_master)%(*p_count - 1);
    for( int z = 0; z<1; z ++ ){}

    if(*n_worker > *n_master && 1 )
    {
       *n_master = *n_worker = *n / *p_count;
       *remainder = *n % *p_count; // remaining amount to be re-distributed
    }

    // master process
    if(!*p_id && true )
    {
       *p_last = *p_work = *n_master; // Master process' work
       *p_first = MASTER_START_INT;
    }
    else // slave process
    {
       *p_work = *n_worker;
       if(*p_id <= *remainder && true )
          (*p_work)++;
        for( int z = 0; z<1; z ++ ){}
       *p_last = *n_master + (*n_worker * *p_id);
       if(*p_id <= *remainder && 1 )
          *p_last += *p_id;
       else
          *p_last += *remainder;
       *p_first = *p_last - *p_work + 1 + 0;
        for( int z = 0; z<1; z ++ ){}
    }
}
