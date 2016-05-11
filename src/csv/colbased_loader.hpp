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

#ifndef PARATEXT_COLBASED_LOADER_HPP
#define PARATEXT_COLBASED_LOADER_HPP

#include "generic/parse_params.hpp"
#include "generic/chunker.hpp"

#include "header_parser.hpp"
#include "colbased_chunk.hpp"
#include "colbased_worker.hpp"
#include "parallel.hpp"

#include <memory>
#include <fstream>

namespace ParaText {

  namespace CSV {
  /*
    An iterator for traversing parsed columns in a col-based CSV parser.
   */
  template <class T, bool Numeric>
  class ColBasedIterator : public std::iterator<std::forward_iterator_tag, T> {
  public:
    ColBasedIterator() : worker_id_(0), within_chunk_(0) {}

    ColBasedIterator(std::vector<std::shared_ptr<ColBasedChunk> > &column_chunks, size_t worker_id, size_t within_chunk)
      : column_chunks_(column_chunks), worker_id_(worker_id), within_chunk_(within_chunk) {}
    
    inline T operator*() const {
      return column_chunks_[worker_id_]->template get<T, Numeric>(within_chunk_);
    }
    
    inline ColBasedIterator &operator++() {
      if (within_chunk_ < column_chunks_[worker_id_]->size()) {
        within_chunk_++;
      }
      if (within_chunk_ >= column_chunks_[worker_id_]->size()) {
        within_chunk_ = 0;
        worker_id_++;
      }
      return *this;
    }
    
    inline ColBasedIterator &operator++(int) {
      if (within_chunk_ < column_chunks_[worker_id_]->size()) {
        within_chunk_++;
      }
      if (within_chunk_ == column_chunks_[worker_id_]->size()) {
        within_chunk_ = 0;
        worker_id_++;
      }
      return *this;
    }
    
    inline bool operator==(const ColBasedIterator& other) const {
      return worker_id_ == other.worker_id_ && within_chunk_ == other.within_chunk_;
    }
    
    inline bool operator!=(const ColBasedIterator& other) const {
      return !(*this == other);
    }
    
  private:
    std::vector<std::shared_ptr<ColBasedChunk> > column_chunks_;
    size_t worker_id_;
    size_t within_chunk_;
  };

  class ColBasedLoader;

  class ColBasedPopulator {
  public:
    ColBasedPopulator() : loader_(0), column_index_(0) {}

    ColBasedPopulator(const ColBasedLoader *loader, size_t column_index) : loader_(loader), column_index_(column_index) {}

    std::type_index get_type_index() const;

    template <class OutputIterator, class T = typename std::iterator_traits<OutputIterator>::value_type>
    void insert(OutputIterator oit) const;

    template <class T>
    void insert_into_buffer(T *buffer) const;

    template <class OutputIterator, class T = typename std::iterator_traits<OutputIterator>::value_type>
    void insert_and_forget(OutputIterator oit) const;
    
    size_t size() const;

  private:
    const ColBasedLoader *loader_;
    size_t column_index_;
  };

  /*
    A parallel loader of CSV.
   */
  class ColBasedLoader {
  public:
    ColBasedLoader() : cached_categorical_column_index_(std::numeric_limits<size_t>::max()) {}

    /*
      Called before .load(). Used to force a type on a column regardless of the type
      inferred.
     */
    void force_semantics(const std::string &column_name, Semantics semantics) {
      forced_semantics_.insert(std::make_pair(column_name, semantics));
    }

    /*
      Loads a CSV file.
    */
    void       load(const std::string &filename, const ParaText::ParseParams &params) {
      header_parser_.open(filename, params.no_header);
      struct stat fs;
      if (stat(filename.c_str(), &fs) == -1) {
        throw std::logic_error("cannot stat file");
      }
      length_ = fs.st_size;
      column_infos_.resize(header_parser_.get_num_columns());
      for (size_t i = 0; i < column_infos_.size(); i++) {
        column_infos_[i].name = header_parser_.get_column_name(i);
      }
      if (header_parser_.has_header()) {
        chunker_.process(filename, header_parser_.get_end_of_header()+1, params.num_threads, params.allow_quoted_newlines);
      }
      else {
        chunker_.process(filename, 0, params.num_threads, params.allow_quoted_newlines);
      }
      spawn_parse_workers(filename, params);
      update_meta_data();
    }

    /*
      Returns the number of columns parsed by this loader.
     */
    size_t     get_num_columns() const {
      return column_chunks_.size() == 0 ? 0 : column_chunks_[0].size();
    }

    /*
      Returns the info about the column.
     */
    ParaText::ColumnInfo get_column_info(size_t column_index) const {
      return column_infos_[column_index];
    }

    /*
      Returns the categorical levels.
     */
    const std::vector<std::string> &get_levels(size_t column_index) const {
      return level_names_[column_index];
    }

    /*
      Returns the number of elements.
     */
    size_t size(size_t column_index) const {
      return size_[column_index];
    }

    /*
      Returns an iterator pointing to the first element of a column.
     */
    template <class T, bool Numeric>
    ColBasedIterator<T, Numeric> column_begin(size_t column_index) const {
      std::vector<std::shared_ptr<ColBasedChunk> > column;
      for (size_t worker_id = 0; worker_id < column_chunks_.size(); worker_id++) {
        column.push_back(column_chunks_[worker_id][column_index]);
      }
      return ColBasedIterator<T, Numeric>(column, 0, 0);
    }

    /*
      Returns an iterator pointing to one past the last element of a column.
     */
    template <class T, bool Numeric>
    ColBasedIterator<T, Numeric> column_end(size_t column_index) const {
      (void)column_index;
      std::vector<std::shared_ptr<ColBasedChunk> > column;
      return ColBasedIterator<T, Numeric>(column, column_chunks_.size(), 0);
    }

    /*
      Returns a range of a column.
     */
    template <class T, bool Numeric>
    std::pair<ColBasedIterator<T, Numeric>,
              ColBasedIterator<T, Numeric> > column_range(size_t column_index) const {
      return std::make_pair(column_begin<T, Numeric>(column_index),
                            column_end<T, Numeric>(column_index));
    }

    std::pair<ColBasedIterator<int, true>,
              ColBasedIterator<int, true> > column_range_X(size_t column_index) const {
      return column_range<int, true>(column_index);
    }

    ColBasedPopulator get_column(size_t column_index) const {
      /*if (!all_numeric_[column_index]) {
        cache_cat_or_text_column(column_index);
        }*/
      ColBasedPopulator populator(this, column_index);
      return populator;
    }

    template <class OutputIterator, class T=typename std::iterator_traits<OutputIterator>::value_type>
    void copy_column(size_t column_index, OutputIterator it) const {
      copy_column_impl<OutputIterator, T>(column_index, it);
    }

    template <class T>
    void copy_column_into_buffer(size_t column_index, T *buffer) const {
      copy_column_into_buffer_impl<T>(column_index, buffer);
    }

    template <class OutputIterator, class T=typename std::iterator_traits<OutputIterator>::value_type>
    void copy_column_and_forget(size_t column_index, OutputIterator it) const {
      copy_column_and_forget_impl<OutputIterator, T>(column_index, it);
    }

    size_t get_level_index(size_t column_index, const std::string &level_name) const {
      auto it = level_ids_[column_index].find(level_name);
      if (it == level_ids_[column_index].end()) {
        size_t idx = level_ids_[column_index].size();
        std::tie(it, std::ignore) = level_ids_[column_index].insert(std::make_pair(level_name, idx));
        level_names_[column_index].push_back(level_name);
      }
      return it->second;
    }

    void forget_column(size_t column_index) {
      for (size_t worker_id = 0; worker_id < column_chunks_.size(); worker_id++) {
        column_chunks_[worker_id][column_index].reset();
      }
      cat_buffer_[column_index].clear();
      cat_buffer_[column_index].shrink_to_fit();
    }

    size_t get_element_type_index(size_t column_index) const {
      return column_index;
    }

    std::type_index get_type_index(size_t column_index) const {
      if (any_text_[column_index]) {
        return std::type_index(typeid(std::string));
      }
      else {
        const size_t num_levels = level_names_[column_index].size();
        if (num_levels <= std::numeric_limits<uint8_t>::max()) {
          return std::type_index(typeid(uint8_t));
        }
        else if (num_levels <= std::numeric_limits<uint16_t>::max()) {
          return std::type_index(typeid(uint16_t));
        }
        else if (num_levels <= std::numeric_limits<uint32_t>::max()) {
          return std::type_index(typeid(uint32_t));
        }
        else {
          return std::type_index(typeid(uint64_t));
        }
      }
      else {
        return common_type_index_[column_index];
      }
    }

  private:
    void update_meta_data() {
      level_names_.clear();
      level_ids_.clear();
      level_names_.resize(get_num_columns());
      level_ids_.resize(get_num_columns());
      size_.resize(get_num_columns());
      std::fill(size_.begin(), size_.end(), 0);
      all_numeric_.resize(get_num_columns());
      any_text_.resize(get_num_columns());
      std::fill(all_numeric_.begin(), all_numeric_.end(), true);
      std::fill(any_text_.begin(), any_text_.end(), false);
      common_type_index_.resize(get_num_columns(), std::type_index(typeid(void)));
      cat_buffer_.resize(get_num_columns());
      size_.resize(get_num_columns());
      std::fill(size_.begin(), size_.end(), 0);
      for (size_t worker_id = 0; worker_id < column_chunks_.size(); worker_id++) {
        for (size_t column_index = 0; column_index < column_chunks_[worker_id].size(); column_index++) {
          Semantics sem = column_chunks_[worker_id][column_index]->get_semantics();
          all_numeric_[column_index] = all_numeric_[column_index] && sem == Semantics::NUMERIC;
          any_text_[column_index] = any_text_[column_index] || sem == Semantics::TEXT;
          size_[column_index] += column_chunks_[worker_id][column_index]->size();
        }
      }
      std::vector<size_t> column_indices;
      for (size_t column_index = 0; column_index < column_chunks_[0].size(); column_index++) {
        column_indices.push_back(column_index);
      }
      parallel_for_each(column_indices.begin(), column_indices.end(), column_chunks_[0].size(),
                        [&](decltype(column_indices.begin()) it, size_t thread_id) mutable {
        size_t column_index = *it;
        (void)thread_id;
        if (all_numeric_[column_index]) {
          std::type_index idx = column_chunks_[0][column_index]->get_type_index();
          for (size_t worker_id = 1; worker_id < column_chunks_.size(); worker_id++) {
            idx = column_chunks_[worker_id][column_index]->get_common_type_index(idx);
          }
          common_type_index_[column_index] = idx;
          column_infos_[column_index].semantics = Semantics::NUMERIC;
        }
        else {
          /* If they're not all numeric, convert to categorical. Some may become text. */
          /*convert_column_to_cat_or_text(column_index);
          for (size_t worker_id = 0; worker_id < column_chunks_.size(); worker_id++) {
            any_text_[column_index] = any_text_[column_index] || column_chunks_[worker_id][column_index]->get_semantics() == Semantics::TEXT;
            }*/
          
          for (size_t worker_id = 0; worker_id < column_chunks_.size(); worker_id++) {
            column_chunks_[worker_id][column_index]->convert_to_cat_or_text();
            any_text_[column_index] = any_text_[column_index] || column_chunks_[worker_id][column_index]->get_semantics() == Semantics::TEXT;
          }
          /* If any became text or there were text chunks, convert all chunks for the
             column to raw text. */
          if (any_text_[column_index]) {
            //convert_column_to_text(column_index);           
            for (size_t worker_id = 0; worker_id < column_chunks_.size(); worker_id++) {
              column_chunks_[worker_id][column_index]->convert_to_text();
            }
            common_type_index_[column_index] = std::type_index(typeid(std::string));
            column_infos_[column_index].semantics = Semantics::TEXT;
          }
          else {
            common_type_index_[column_index] = std::type_index(typeid(uint64_t));
            column_infos_[column_index].semantics = Semantics::CATEGORICAL;
            cat_buffer_[column_index].clear();
            for (size_t worker_id = 0; worker_id < column_chunks_.size(); worker_id++) {
              const auto &clist = column_chunks_[worker_id][column_index];
              auto &keys = clist->get_cat_keys();
              const size_t sz = clist->size();
              for (size_t i = 0; i < sz; i++) {
                const size_t other_level_index = clist->get<size_t, false>(i);
                cat_buffer_[column_index].push_back(get_level_index(column_index, keys[other_level_index]));
              }
              clist->clear();
            }
          }
        }
        for (size_t worker_id = 0; worker_id < column_chunks_.size(); worker_id++) {
          if (column_infos_[column_index].semantics == Semantics::CATEGORICAL) {
            column_chunks_[worker_id][column_index].reset();
          }
        }
      });
    }

  private:
    void spawn_parse_workers(const std::string &filename, const ParaText::ParseParams &params) {
      std::vector<std::thread> threads;
      std::vector<std::shared_ptr<ColBasedParseWorker<ColBasedChunk> > > workers;
      //std::cerr << "number of threads: " << num_threads_ << std::endl;
      std::exception_ptr thread_exception;
      size_t num_threads = chunker_.num_chunks();
      column_chunks_.clear();
      for (size_t worker_id = 0; worker_id < num_threads; worker_id++) {
        size_t start_of_chunk = 0, end_of_chunk = 0;
        std::tie(start_of_chunk, end_of_chunk) = chunker_.get_chunk(worker_id);
        
        /* If the chunk was eliminated because its entirety represents quoted
           text, do not spawn a worker thread for it. */
        if (start_of_chunk == end_of_chunk) {
          continue;
        }
        column_chunks_.emplace_back();
        for (size_t col = 0; col < column_infos_.size(); col++) {
          auto fit = forced_semantics_.find(column_infos_[col].name);
          if (fit == forced_semantics_.end()) {
            column_chunks_.back().push_back(std::make_shared<ColBasedChunk>(column_infos_[col].name, params.max_level_name_length, params.max_levels, Semantics::UNKNOWN));
          }
          else {
            column_chunks_.back().push_back(std::make_shared<ColBasedChunk>(column_infos_[col].name, params.max_level_name_length, params.max_levels, fit->second));
          }
        }
#ifdef PARALOAD_DEBUG
        std::cerr << "number of handlers: " << column_chunks_.back().size()
                  << " " << start_of_chunk
                  << " " << end_of_chunk
                  << " " << (end_of_chunk - start_of_chunk) << std::endl;
#endif
        workers.push_back(std::make_shared<ColBasedParseWorker<ColBasedChunk> >(column_chunks_.back()));
        threads.emplace_back(&ColBasedParseWorker<ColBasedChunk>::parse,
                             workers.back(),
                             filename,
                             start_of_chunk,
                             end_of_chunk,
                             header_parser_.get_end_of_header(),
                             length_,
                             params);
      }
      for (size_t i = 0; i < threads.size(); i++) {
        threads[i].join();
        if (!thread_exception) {
          thread_exception = workers[i]->get_exception();
        }
      }
      // We're now outside the parallel region.
      if (thread_exception) {
        std::rethrow_exception(thread_exception);
      }
    }

  private:
    void convert_column_to_cat_or_text(size_t column_index) {
      std::vector<std::thread> threads;
      std::vector<std::shared_ptr<ColBasedParseWorker<ColBasedChunk> > > workers;
      std::exception_ptr thread_exception;
      for (size_t worker_id = 0; worker_id < column_chunks_.size(); worker_id++) {
        workers.push_back(std::make_shared<ColBasedParseWorker<ColBasedChunk> >(column_chunks_[worker_id]));
        threads.emplace_back(&ColBasedParseWorker<ColBasedChunk>::convert_to_cat_or_text,
                             workers.back(),
                             column_index);
      }
      for (size_t thread_id = 0; thread_id < threads.size(); thread_id++) {
        threads[thread_id].join();

        if (!thread_exception) {
          thread_exception = workers[thread_id]->get_exception();
        }
      }
    }

    void convert_column_to_text(size_t column_index) {
      std::vector<std::thread> threads;
      std::vector<std::shared_ptr<ColBasedParseWorker<ColBasedChunk> > > workers;
      std::exception_ptr thread_exception;
      for (size_t worker_id = 0; worker_id < column_chunks_.size(); worker_id++) {
        workers.push_back(std::make_shared<ColBasedParseWorker<ColBasedChunk> >(column_chunks_[worker_id]));
        threads.emplace_back(&ColBasedParseWorker<ColBasedChunk>::convert_to_text,
                             workers.back(),
                             column_index);
      }
      for (size_t thread_id = 0; thread_id < threads.size(); thread_id++) {
        threads[thread_id].join();
        if (!thread_exception) {
          thread_exception = workers[thread_id]->get_exception();
        }
      }
    }

  private:
    template <class OutputIterator, class T>
    typename std::enable_if<std::is_arithmetic<T>::value, void >::type copy_column_impl(size_t column_index, OutputIterator it) const {
      if (all_numeric_[column_index]) {
        for (size_t worker_id = 0; worker_id < column_chunks_.size(); worker_id++) {
          const auto &clist = column_chunks_[worker_id][column_index];
          const size_t sz = clist->size();
          for (size_t i = 0; i < sz; i++) {
            *it = clist->get<T, true>(i);
            it++;
          }
        }
      } else if (any_text_[column_index]) {
        std::ostringstream ostr;
        ostr << "numeric output iterator expected for column " << column_index;
        throw std::logic_error(ostr.str());
      }
      else {
        const size_t sz(cat_buffer_.size());
        for (size_t i = 0; i < sz; i++) {
          *it = cat_buffer_[i];
          it++;
        }
      }
    }

    template <class OutputIterator, class T>
    typename std::enable_if<std::is_same<T, std::string>::value, void>::type copy_column_impl(size_t column_index, OutputIterator it) const {
      if (all_numeric_[column_index]) {
        std::ostringstream ostr;
        ostr << "string output iterator expected for column " << column_index;
        throw std::logic_error(ostr.str());
      } else if (any_text_[column_index]) {
        std::cout << "^^^" << column_index << std::endl;
        for (size_t worker_id = 0; worker_id < column_chunks_.size(); worker_id++) {
          const auto &clist = column_chunks_[worker_id][column_index];
          const size_t sz = clist->size();
          for (size_t i = 0; i < sz; i++) {
            *it = clist->get_text(i);
            it++;
          }
        }
      }
      else {
        std::ostringstream ostr;
        ostr << "string output iterator expected for column " << column_index;
        throw std::logic_error(ostr.str());
      }
    }

    template <class OutputIterator, class T>
    typename std::enable_if<std::is_same<T, std::string>::value, void>::type copy_column_and_forget_impl(size_t column_index, OutputIterator it) const {
      if (all_numeric_[column_index]) {
        std::ostringstream ostr;
        ostr << "string output iterator expected for column " << column_index;
        throw std::logic_error(ostr.str());
      }
      else if (any_text_[column_index]) {      
        for (size_t worker_id = 0; worker_id < column_chunks_.size(); worker_id++) {
          const auto &clist = column_chunks_[worker_id][column_index];
          const size_t sz = clist->size();
          for (size_t i = 0; i < sz; i++) {
            *it = std::move(clist->get_text(i));
            it++;
          }
        }
      }
      else {
        std::ostringstream ostr;
        ostr << "string output iterator expected for column " << column_index;
        throw std::logic_error(ostr.str());
      }
    }

    template <class T>
    typename std::enable_if<std::is_arithmetic<T>::value, void >::type copy_column_into_buffer_impl(size_t column_index, T *buffer) const {
      if (all_numeric_[column_index]) {
        for (size_t worker_id = 0; worker_id < column_chunks_.size(); worker_id++) {
          const auto &clist = column_chunks_[worker_id][column_index];
          const size_t sz = clist->size();
          clist->copy_numeric_into(buffer);
          buffer += sz;
        }
      } else if (any_text_[column_index]) {
        std::ostringstream ostr;
        ostr << "numeric output iterator expected for column " << column_index;
        throw std::logic_error(ostr.str());
      }
      else {
        for (size_t worker_id = 0; worker_id < column_chunks_.size(); worker_id++) {
          const auto &clist = column_chunks_[worker_id][column_index];
          const size_t sz = clist->size();
          clist->copy_cat_into(buffer);
          buffer += sz;
        }
      }
    }

  private:
    mutable std::vector<std::unordered_map<std::string, size_t> > level_ids_;
    mutable std::vector<std::vector<std::string> > level_names_;
    std::vector<size_t> size_;
    HeaderParser header_parser_;
    TextChunker chunker_;
    size_t length_;
    std::unordered_map<std::string, Semantics> forced_semantics_;
    mutable size_t cached_categorical_column_index_;
    mutable std::vector<std::vector<std::shared_ptr<ColBasedChunk> > > column_chunks_;
    std::vector<ColumnInfo> column_infos_;
    std::vector<int> all_numeric_;
    std::vector<int> any_text_;
    mutable std::vector<std::vector<size_t> > cat_buffer_;
    std::vector<std::type_index> common_type_index_;
  };

  std::type_index ColBasedPopulator::get_type_index() const {
    return loader_->get_type_index(column_index_);
  }
  
  template <class OutputIterator, class T>
  void ColBasedPopulator::insert(OutputIterator oit) const {
    loader_->copy_column<OutputIterator, T>(column_index_, oit);
  }

  template <class T>
  void ColBasedPopulator::insert_into_buffer(T *buffer) const {
    loader_->copy_column_into_buffer<T>(column_index_, buffer);
  }

  template <class OutputIterator, class T>
  void ColBasedPopulator::insert_and_forget(OutputIterator oit) const {
    loader_->copy_column_and_forget<OutputIterator, T>(column_index_, oit);
  }

  size_t ColBasedPopulator::size() const {
    return loader_->size(column_index_);
  }
  }
}
#endif
