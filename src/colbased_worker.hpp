#ifndef WISEIO_PARSE_WORKER_COL_BASED_HPP
#define WISEIO_PARSE_WORKER_COL_BASED_HPP

#include <fstream>
#include "strings.hpp"
#include "widening_vector.hpp"

namespace ParaText {

template <class ColumnHandler>
class ColBasedParseWorker {
public:
  ColBasedParseWorker(std::vector<std::shared_ptr<ColumnHandler> > &handlers) : handlers_(handlers), lines_parsed_(0), quote_started_('\0'), column_index_(0), escape_started_(false) {}

  virtual ~ColBasedParseWorker() {}

  void parse(const std::string &filename,
             size_t begin,
             size_t end,
             size_t data_begin,
             size_t file_end,
             const ParaText::ParseParams &params) {
    if (params.number_only) {
      parse_impl<true>(filename, begin, end, data_begin, file_end, params);
    }
    else {
      parse_impl<false>(filename, begin, end, data_begin, file_end, params);
    }
  }

  template <bool NumberOnly>
  void parse_impl(const std::string &filename,
                  size_t begin,
                  size_t end,
                  size_t data_begin,
                  size_t file_end,
                  const ParaText::ParseParams &params) {
    (void)data_begin;
    (void)file_end;
    std::ifstream in;
    in.open(filename.c_str());
    column_index_ = 0;
    quote_started_ = '\0';
    escape_started_ = false;
    size_t current = begin;
    //size_t block_size = 65536;
    const size_t block_size = params.block_size;
    //size_t block_size = 4096;
    char buf[block_size];
    in.seekg(current, std::ios_base::beg);
    definitely_string_ = false;
#ifdef PARALOAD_DEBUG
    size_t round = 0;
#endif
    while (current < end) {
      if (current % block_size == 0) { /* The block is aligned. */
        in.read(buf, std::min(end - current + 1, block_size));
      }
      else { /* Our first read should ensure our further reads are block-aligned. */
        in.read(buf, std::min(end - current + 1, std::min(block_size, current % block_size)));
      }
      size_t nread = in.gcount();
#ifdef PARALOAD_DEBUG
      if (round == 0) {
        std::cout << "R{" << std::string((char *)buf, (char *)buf + nread) << std::endl;
      }
      round++;
#endif
      if (nread == 0) {
        break;
      }
      if (NumberOnly) {
        size_t i = 0;
        for (; i < nread; i++) {
          if (buf[i] == ',') {
            process_token();
          }
          else if (buf[i] == '\n') {
            process_token();
            process_newline();
          } else {
            token_.push_back(buf[i]);
          }
        }
      } else {
        for (size_t i = 0; i < nread;) {
          if (quote_started_ != '\0') {
            for (; i < nread; i++) {
              if (buf[i] == quote_started_) {
                i++;
                quote_started_ = '\0';
                break;
              }
              token_.push_back(buf[i]);
            }
          }
          else {
            for (; i < nread; i++) {
              if (buf[i] == '"') {
                i++;
                quote_started_ = '\"';
                definitely_string_ = true;
                break;
              }
              else if (buf[i] == ',') {
                process_token();
              }
              else if (buf[i] == '\n') {
                process_token();
                process_newline();
              }
              else {
                token_.push_back(buf[i]);
              }
            }
          }
        }
      }
      current += nread;
    }
    if (column_index_ + 1 == handlers_.size()) {
      process_token();
      process_newline();
    }
#ifdef PARALOAD_DEBUG
    std::cout << "lines parsed: " << lines_parsed_ << std::endl;
#endif
    return;
  }

  void process_newline() {
    column_index_ = 0;
    lines_parsed_++;
  }

  void process_token_number_only() {
    if (column_index_ >= handlers_.size()) {
      std::ostringstream ostr;
      ostr << "invalid column index: " << column_index_ << " (" << handlers_.size() << " columns)" << std::endl;
      std::string estr(ostr.str());
      throw std::logic_error(estr);
    }
    size_t i = 0;
    for (; i < token_.size() && isspace(token_[i]); i++) {}
    if (i < token_.size()) {
      if (token_[i] == '-') { i++; }
    }
    for (; i < token_.size() && isdigit(token_[i]); i++) {}
    if (i < token_.size()) {
      handlers_[column_index_]->process_float(bsd_strtod(token_.begin(), token_.end()));
    }
    else {
      handlers_[column_index_]->process_integer(fast_atoi<long>(token_.begin(), token_.end()));
    }
    column_index_++;
    token_.clear();
  }

  void process_token() {
    if (column_index_ >= handlers_.size()) {
      std::ostringstream ostr;
      ostr << "invalid column index: " << column_index_ << " (" << handlers_.size() << " columns)" << std::endl;
      std::string estr(ostr.str());
      throw std::logic_error(estr);
    }
    if (definitely_string_) {
      handlers_[column_index_]->process_categorical(token_.begin(), token_.end());
      definitely_string_ = false;
    }
    else {
      size_t i = 0;
      bool integer_possible = false, float_possible = false, exp_possible = false;
      for (; i < token_.size() && isspace(token_[i]); i++) {}
      if (i < token_.size()) {
        if (token_[i] == '-') {
          i++;
        }
        bool integer_possible = std::isdigit(token_[i]);
        i++;
        float_possible = integer_possible, exp_possible = integer_possible;
        while (i < token_.size() && integer_possible) {
          integer_possible = isdigit(token_[i]);
          i++;
        }
      }
      if (i < token_.size()) {
        integer_possible = false;
        float_possible = token_[i] == '.';
        i++;
        while (i < token_.size() && float_possible) {
          float_possible = isdigit(token_[i]);
          i++;
        }
        if (float_possible && i < token_.size()) {
          float_possible = false;
          exp_possible = token_[i] == 'E' || token_[i] == 'e';
          i++;
          if (exp_possible && i < token_.size()) {
            std::cout << "A";
            if (token_[i] == '+' || token_[i] == '-') {
              std::cout << "B";
              i++;
              if (i < token_.size()) {
                std::cout << "C";
                exp_possible = isdigit(token_[i]);
                i++;
                while (i < token_.size() && exp_possible) {
                  exp_possible = isdigit(token_[i]);
                  i++;
                }
              }
              else {
                exp_possible = false;
              }
            }
            else if (isdigit(token_[i])) {
              std::cout << "D";
              while (i < token_.size() && exp_possible) {
                exp_possible = isdigit(token_[i]);
                i++;
              }
              std::cout << "E" << exp_possible << (token_[i-1]);
            }
            else {
              exp_possible = false;
            }
          }
          else {
            exp_possible = false;
          }
        }
      }
      if (integer_possible) {
        handlers_[column_index_]->process_integer(fast_atoi<long>(token_.begin(), token_.end()));
      }
      else if (float_possible || exp_possible) {
        handlers_[column_index_]->process_float(bsd_strtod(token_.begin(), token_.end()));
      }
      else {
        handlers_[column_index_]->process_categorical(token_.begin(), token_.end());
      }
    }
    column_index_++;
    token_.clear();
  }

private:
  std::vector<std::shared_ptr<ColumnHandler> > handlers_;
  std::vector<char>                            token_;
  bool                                         definitely_string_;
  size_t                                       lines_parsed_;
  char                                         quote_started_;
  size_t                                       column_index_;
  bool                                         escape_started_;
};
}

#endif
