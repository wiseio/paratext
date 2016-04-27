/*

    ParaText: parallel text reading
    Copyright (C) 2016. wise.io, Inc.

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

/*
  Coder: Damian Eads.
 */

#ifndef PARATEXT_HEADER_PARSER_HPP
#define PARATEXT_HEADER_PARSER_HPP

#include <vector>
#include <fstream>
#include <unordered_set>

namespace ParaText {

namespace CSV {

  /*
    Parses the first line of a CSV file to determine the header.
   */
  class HeaderParser {
  public:
    /*
      Constructs an uninitialized header parser.
     */
    HeaderParser() : length_(0), end_of_header_(0), has_header_(false) {}

    /*
      Destroys this parser.
     */
    virtual ~HeaderParser() {}

    /*
      Opens a file and parses its header.
     */
    void open(const std::string &filename, bool no_header) {
      struct stat fs;
      if (stat(filename.c_str(), &fs) == -1) {
        std::ostringstream ostr;
        ostr << "cannot open file '" << filename << "'";
        throw std::logic_error(ostr.str());
      }
      length_ = fs.st_size;
      in_.open(filename);
      if (!in_) {
        std::ostringstream ostr;
        ostr << "cannot open file '" << filename << "'";
        throw std::logic_error(ostr.str());
      }
      parse_header(no_header);
    }

    /*
      Returns the number of columns detected in the header.
     */
    size_t get_num_columns() const {
      return column_names_.size();
    }
    
    /*
      Adds a column of a specified name.
     */
    void add_column_name(const std::string &name) {
      //std::cerr << "col " << column_names_.size() << ": " << name << std::endl;
      column_names_.push_back(name);
    }
    
    /*
      Returns a specific name of a column.
     */
    const std::string &get_column_name(size_t index) const {
      return column_names_[index];
    }
    
    /*
      Parses a header.
     */
    void parse_header(bool no_header=false) {
      std::string token;
      size_t current = 0;
      size_t block_size = 4096;
      char buf[block_size];
      char quote_started = 0;
      in_.seekg(0, std::ios_base::beg);
      while (current < length_) {
        if (current % block_size == 0) { /* The block is aligned. */
          in_.read(buf, std::min(length_ - current, block_size));
        }
        else { /* Our first read should ensure our further reads are block-aligned. */
          in_.read(buf, std::min(length_ - current, std::min(block_size, current % block_size)));
        }
        size_t nread = in_.gcount();
        size_t i = 0;
        if (quote_started) {
          for (; i < nread; i++) {
            if (buf[i] == quote_started) {
              quote_started = 0;
              break;
            }
            token.push_back(buf[i]);
          }
        }
        for (; i < nread; i++) {
          switch (buf[i]) {
          case '\'':
          case '\"':
            {
              quote_started = buf[i];
              i++;
              for (; i < nread; i++) {
                if (buf[i] == quote_started) {
                  quote_started = false;
                  break;
                }
                token.push_back(buf[i]);
              }
            }
            //std::cout << token << std::endl;
            break;
          case ',':
            add_column_name(token);
            token.clear();
            break;
          case '\n':
            add_column_name(token);
            token.clear();
            end_of_header_ = current + i;
            goto header_finished;
            break;
          default:
            token.push_back(buf[i]);
            break;
          }
        }
        current += nread;
      }
    header_finished:
      std::unordered_set<std::string> cnset;
      for (auto &cname : column_names_) {
        cnset.insert(cname);
      }
      has_header_ = true;
      if (cnset.size() != column_names_.size() || no_header) {
        has_header_ = false;
#ifdef PARALOAD_DEBUG
        std::cout << "column names not unique: " << cnset.size() << " unique column names found." ;
#endif
        size_t num_columns = column_names_.size();
        column_names_.clear();
        for (size_t i = 0; i < num_columns; i++) {
          std::ostringstream ostr;
          ostr << "col" << i;
          std::string sstr(ostr.str());
          column_names_.push_back(sstr);
        }
        end_of_header_ = 0;
      }
#ifdef PARALOAD_DEBUG
      std::cout << "Total columns in header: " << column_names_.size() << std::endl;
#endif
      return;
    }

    /*
      Returns the end of the header.
     */
    size_t get_end_of_header() const {
      return end_of_header_;
    }

    bool has_header() const {
      return has_header_;
    }

  private:
    std::ifstream in_;
    std::vector<std::string> column_names_;
    size_t length_;
    size_t end_of_header_;
    bool has_header_;
  };
}
}
#endif
