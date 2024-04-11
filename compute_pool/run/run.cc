// Author: Ming Zhang
// Copyright (c) 2022

#include "worker/handler.h"
#include "worker/global.h"
#include <brpc/channel.h>

// Entrance to run threads that spawn coroutines as coordinators to run distributed transactions
int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  std::cout << "init parameter: read only txn rate:" << FLAGS_READONLY_TXN_RATE << std::endl; 
  assert(FLAGS_READONLY_TXN_RATE >= 0 && FLAGS_READONLY_TXN_RATE <= 1);
  std::this_thread::sleep_for(std::chrono::seconds(2));

  if (argc < 3) {
    std::cerr << "./run <benchmark_name> <system_name> <thread_num>(optional) <coroutine_num>(optional) <cache_size_GB>(optional) . E.g., ./run tatp ford 16 8" << std::endl;
    return 0;
  }

  Handler* handler = new Handler();
  handler->ConfigureComputeNode(argc, argv);
  handler->GenThreads(std::string(argv[1]));
  handler->OutputResult(std::string(argv[1]), std::string(argv[2]));
}
