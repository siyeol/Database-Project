#include "bench.h"

namespace sqliteBench {
  
  // #1. Write a code for setting the journal mode in the SQLite database engine
  // [Requirement]
  // (1) This function set jounral mode using "FLAGS_journal_mode" extern variable (user input)
  // (2) This function check journal mode; if user try to set wrong journal mode then return -2
  // (3) This function returns status (int data type) of sqlite API function
  int Benchmark::benchmark_setJournalMode() {
    /*
    Used strcasecmp to be invariant with Upper/Lower cases
    if not in 6 modes, return -2
    */
    char* err_msg = NULL;

    if (strcasecmp(FLAGS_journal_mode,"DELETE")==0){
      return sqlite3_exec(db_, "PRAGMA journal_mode = DELETE", NULL, NULL, &err_msg);
    } else if (strcasecmp(FLAGS_journal_mode,"TRUNCATE")==0){
      return sqlite3_exec(db_, "PRAGMA journal_mode = TRUNCATE", NULL, NULL, &err_msg);
    } else if (strcasecmp(FLAGS_journal_mode,"PERSIST")==0){
      return sqlite3_exec(db_, "PRAGMA journal_mode = PERSIST", NULL, NULL, &err_msg);
    } else if (strcasecmp(FLAGS_journal_mode,"MEMORY")==0){
      return sqlite3_exec(db_, "PRAGMA journal_mode = MEMORY", NULL, NULL, &err_msg);
    } else if (strcasecmp(FLAGS_journal_mode,"WAL")==0){
      return sqlite3_exec(db_, "PRAGMA journal_mode = WAL", NULL, NULL, &err_msg);
    } else if (strcasecmp(FLAGS_journal_mode,"OFF")==0){
      return sqlite3_exec(db_, "PRAGMA journal_mode = OFF", NULL, NULL, &err_msg);
    } else{
      return -2;
    }   
  }

  // #2. Write a code for setting page size in the SQLite database engine
  // [Requirement]
  // (1) This function set page size using "FLAGS_page_size" extern variable (user input)
  // (2) This function return status (int data type) of sqltie API function
  // (3) This function is called at benchmark_open() in bench.cc
  int Benchmark::benchmark_setPageSize() {
    /*
    with snprintf, insert FLAGS_page_size into schema.page_size
    returning status of sqlite3_exec
    */
    int status;
    char page_size[100];
    char* err_msg = NULL;
    snprintf(page_size, sizeof(page_size), "PRAGMA schema.page_size = %d", FLAGS_page_size);
    status = sqlite3_exec(db_, page_size, NULL, NULL, &err_msg);
    // please fill this function
    return status;

  }

  // #3. Write a code for insert function (direct SQL execution mode)
  // [Requriement]
  // (1) This function fill random key-value data using direct qurey execution sqlite API function
  //     (please refer to lecture slide project 3)
  // (2) This function has single arguemnt num_ which is total number of operations
  // (3) This function create SQL statement (key-value pair) do not modfiy this code 
  // (4) This function execute given SQL statement
  // (5) This function is called at benchmark_open() in bench.cc
  int Benchmark::benchmark_directFillRand(int num_) {
    //      DO NOT MODIFY HERE     //
    const char* value = gen_.rand_gen_generate(FLAGS_value_size);
    char key[100];
    const int k = gen_.rand_next() % num_;

    snprintf(key, sizeof(key), "%016d", k);
    char fill_stmt[100];
    snprintf(fill_stmt, sizeof(fill_stmt), "INSERT INTO test values (%s , x'%x')", key ,value);
    ////////////////////////////////
    /*
    With the given num_, iterate and generate random values.
    Then Insert into test value with key-value.
    */
    for(int i =num_;i>0;i--){
      const char* _value = gen_.rand_gen_generate(FLAGS_value_size);
      char _key[100];
      const int _k = gen_.rand_next() % i ;

      snprintf(_key, sizeof(_key), "%016d", _k);
      char _fill_stmt[100];
      snprintf(_fill_stmt, sizeof(_fill_stmt), "INSERT INTO test values (%s , x'%x')", _key ,_value);
      sqlite3_exec(db_, _fill_stmt, NULL, NULL, NULL);
      done_++;
    }
    // execute SQL statement
    // please fill this function
    return 0;
  }

  // xxx(homework)
  // write your own benchmark functions
  // you can add multiple functions as you like 
  // you can change function name. Here example is literally example.
  int Benchmark::benchmark_example(int num_) {
    /*
    Read Intensive Benchmark
    Since WAL might be slightly slower in read intensive applications,
    this benchmark is testing the read intensive situations.
    */
    char* err_msg = NULL;
    
    for(int i =num_;i>0;i--){
      const char* _value = gen_.rand_gen_generate(FLAGS_value_size);
      char _key[100];
      const int _k = gen_.rand_next() % i ;

      snprintf(_key, sizeof(_key), "%016d", _k);
      char _fill_stmt[100];
      snprintf(_fill_stmt, sizeof(_fill_stmt), "INSERT INTO test values (%s , x'%x')", _key ,_value);
      sqlite3_exec(db_, _fill_stmt, NULL, NULL, NULL);
      done_++;
    }

    for(int i = 0; i < num_*num_ ;i++){
      sqlite3_exec(db_, "SELECT * FROM test", NULL, NULL, &err_msg);
    }
    return 0;
  }

}; // namespace sqliteBench

