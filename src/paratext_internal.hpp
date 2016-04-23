#ifndef PARATEXT_PARACSV_HPP
#define PARATEXT_PARACSV_HPP

#include <vector>
#include <string>

#include "parse_params.hpp"

void paracsv_load(const std::string &filename, const ParaText::ParseParams &params);

size_t get_num_cores();

#endif
