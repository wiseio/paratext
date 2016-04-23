#include "paratext_internal.hpp"

#include <numpy/arrayobject.h>
#include <numpy/npy_math.h>
#include <string>
#include <type_traits>
#include <iostream>
#include <thread>

size_t get_num_cores() {
  return std::thread::hardware_concurrency();
}
