#include "paratext_internal.hpp"

#include <string>
#include <type_traits>
#include <iostream>
#include <thread>

size_t get_num_cores() {
  return std::thread::hardware_concurrency();
}
