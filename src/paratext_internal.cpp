#include "paratext_internal.hpp"

#include <numpy/arrayobject.h>
#include <numpy/npy_math.h>
#include <string>
#include <type_traits>
#include <iostream>
#include <thread>

void paracsv_load(const std::string &filename, const ParaText::ParseParams &params) {
  std::cout << "paracsv_load" << filename << std::endl;
}

size_t get_num_cores() {
  return std::thread::hardware_concurrency();
}
