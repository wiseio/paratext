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

  Unused code. Work-in-progress. Unfinished.
 */

#ifndef PARATEXT_COLBASED_HOLDER_HPP
#define PARATEXT_COLBASED_HOLDER_HPP

#include "util/widening_vector.hpp"

namespace ParaText {

  namespace CSV {

    class ColHolder;

    ColHolder *create_ColHolder(Semantics, const ColParams &);

    class ColHolder {
    public:
      ColHolder() {}
      virtual ~ColHolder() {}

      virtual ColHolder *add_string(const std::vector<char> &token) = 0;
      virtual ColHolder *add_string(const std::string &token) = 0;
      virtual ColHolder *add_number(uint8_t val) = 0;
      virtual ColHolder *add_number(uint16_t val) = 0;
      virtual ColHolder *add_number(uint32_t val) = 0;
      virtual ColHolder *add_number(uint64_t val) = 0;
      virtual ColHolder *add_number(int8_t val) = 0;
      virtual ColHolder *add_number(int16_t val) = 0;
      virtual ColHolder *add_number(int32_t val) = 0;
      virtual ColHolder *add_number(int64_t val) = 0;
      virtual ColHolder *add_number(float val) = 0;
      virtual ColHolder *add_number(double val) = 0;

      virtual Semantics get_semantics() const = 0;

      //virtual const std::vector<std::string> &get_levels() const = 0;
      virtual std::type_index get_type_index() const = 0;

      virtual void copy_into(uint8_t* array) const = 0;
      virtual void copy_into(uint16_t* array) const = 0;
      virtual void copy_into(uint32_t* array) const = 0;
      virtual void copy_into(uint64_t* array) const = 0;
      virtual void copy_into(int8_t* array) const = 0;
      virtual void copy_into(int16_t* array) const = 0;
      virtual void copy_into(int32_t* array) const = 0;
      virtual void copy_into(int64_t* array) const = 0;
      virtual void copy_into(float* array) const = 0;
      virtual void copy_into(double* array) const = 0;

      virtual size_t size() const = 0;

      virtual ColHolder* combine(ColHolder *other) = 0;
      virtual ColHolder* clone_empty() = 0;
    };

    template <class HolderType>
    class CRTPHolder : public ColHolder {
    public:
      CRTPHolder() {}

      virtual void copy_into(uint8_t* array) const { ((HolderType*)this)->copy_into_impl(array); }
      virtual void copy_into(uint16_t* array) const { ((HolderType*)this)->copy_into_impl(array); }
      virtual void copy_into(uint32_t* array) const { ((HolderType*)this)->copy_into_impl(array); }
      virtual void copy_into(uint64_t* array) const { ((HolderType*)this)->copy_into_impl(array); }
      virtual void copy_into(int8_t* array) const { ((HolderType*)this)->copy_into_impl(array); }
      virtual void copy_into(int16_t* array) const { ((HolderType*)this)->copy_into_impl(array); }
      virtual void copy_into(int32_t* array) const { ((HolderType*)this)->copy_into_impl(array); }
      virtual void copy_into(int64_t* array) const { ((HolderType*)this)->copy_into_impl(array); }
      virtual void copy_into(float* array) const { ((HolderType*)this)->copy_into_impl(array); }
      virtual void copy_into(double* array) const { ((HolderType*)this)->copy_into_impl(array); }
      virtual ColHolder *add_number(float val) { return ((HolderType*)this)->add_number_impl(val); } 
      virtual ColHolder *add_number(double val) { return ((HolderType*)this)->add_number_impl(val); } 
      virtual ColHolder *add_number(uint8_t val) { return ((HolderType*)this)->add_number_impl(val); }
      virtual ColHolder *add_number(uint16_t val) { return ((HolderType*)this)->add_number_impl(val); }
      virtual ColHolder *add_number(uint32_t val) { return ((HolderType*)this)->add_number_impl(val); }
      virtual ColHolder *add_number(uint64_t val) { return ((HolderType*)this)->add_number_impl(val); }
      virtual ColHolder *add_number(int8_t val) { return ((HolderType*)this)->add_number_impl(val); }
      virtual ColHolder *add_number(int16_t val) { return ((HolderType*)this)->add_number_impl(val); }
      virtual ColHolder *add_number(int32_t val) { return ((HolderType*)this)->add_number_impl(val); }
      virtual ColHolder *add_number(int64_t val) { return ((HolderType*)this)->add_number_impl(val); }
    };

    class TextColHolder : public CRTPHolder<TextColHolder> {
    public:
      TextColHolder() {}

      virtual ~TextColHolder() {}
      
      virtual ColHolder *add_string(const std::string &token) {
        data_.emplace_back(token);
        return this;
      }

      virtual ColHolder *add_string(const std::vector<char> &token) {
        data_.emplace_back(token.begin(), token.end());
        return this;
      }

      template <class T>
      ColHolder *add_number_impl(const T &val) {
        data_.push_back(std::move(std::to_string(val)));
        return this;
      }

      virtual std::type_index get_type_index() const {
        return std::type_index(typeid(std::string));
      }

      virtual ColHolder *convert_to_text() {
        return this;
      }

      template <class T>
      inline void copy_into_impl(T *output) const {
        for (size_t i = 0; i < data_.size(); i++) {
          output[i] = 0;
        }
      }

      virtual ColHolder* combine(ColHolder *other) {
        ColHolder *retval = this;
        if (other->get_semantics() == Semantics::NUMERIC || other->get_semantics() == Semantics::CATEGORICAL) {
          std::unique_ptr<ColHolder> A(clone_empty());
          /// All combine()'s when A is of type TextColHolder are guaranteed to return a TextColHolder.
          ColHolder *B = other->combine(A.get());
          if (A.get() != B) {
            A.reset(B);
          }
          /// There are no specializations of TextColHolder so we can cast like this.
          TextColHolder *C = (TextColHolder*)B;
          data_.insert(data_.begin() + data_.size(), C->data_.begin(), C->data_.end());
        }
        else if (other->get_semantics() == Semantics::TEXT) {
          /// There are no specializations of TextColHolder so we can cast like this.
          TextColHolder *C = (TextColHolder*)other;
          data_.insert(data_.begin() + data_.size(), C->data_.begin(), C->data_.end());
        }
        return retval;
      }

      virtual ColHolder* clone_empty() {
        return new TextColHolder();
      }

      virtual Semantics get_semantics() const {
        return Semantics::TEXT;
      }

      virtual size_t size() const {
        return data_.size();
      }

    private:
      std::vector<std::string> data_;
    };
    
    template <class ForceType, bool ForceNumeric>
    class NumColHolder : public CRTPHolder<NumColHolder<ForceType, ForceNumeric> > {
    public:
      NumColHolder(const ColParams &col_params) : col_params_(col_params) {}

      virtual ~NumColHolder() {}
      
      virtual ColHolder *add_string(const std::vector<char> &token) {
        return add_string_impl<ForceNumeric>(token);
      }

      virtual ColHolder *add_string(const std::string &token) {
        return add_string_impl<ForceNumeric>(token);
      }
      
      template <bool ForceNumeric_>
      typename std::enable_if<!ForceNumeric_
      && std::is_arithmetic<ForceType>::value, ColHolder*>::type
        add_string_impl(const std::vector<char> &token) {
        std::unique_ptr<TextColHolder> holder;
        for (size_t i = 0; i < data_.size(); i++) {
          TextColHolder *other = holder->add_number(data_[i]);
          if (other != holder.get()) {
            holder.reset(other);
          }
        }
        {
          TextColHolder *other = holder->add_string(token);
          if (other != holder.get()) {
            holder.reset(other);
          }
        }
        return holder.release();
      }
            
      template <bool ForceNumeric_>
      typename std::enable_if<ForceNumeric_, ColHolder*>::type add_string_impl(const std::vector<char> &token) {
        data_.push_back(parse_num<ForceType>(token.begin(), token.end()));
        return this;
      }
      
      virtual ColHolder *add_float(float val) {
        data_.push_back((ForceType)val);
        return this;
      };
      
      virtual ColHolder *add_integer(long val) {
        data_.push_back((ForceType)val);
        return this;
      }

      virtual ColHolder *convert_to_cat() const {
        std::unique_ptr<ColHolder> holder(create_ColHolder(Semantics::CATEGORICAL, col_params_));
        for (size_t i = 0; i < data_.size(); i++) {
          ColHolder *other = holder->add_number(data_[i]);
          if (other != holder.get()) {
            holder.reset(other);
          }
        }
        return this;
      }

      virtual ColHolder *convert_to_text() const {
        std::unique_ptr<ColHolder> holder(new TextColHolder());
        for (size_t i = 0; i < data_.size(); i++) {
          ColHolder *other = holder->add_number(data_[i]);
          if (other != holder.get()) {
            holder.reset(other);
          }
        }
        return this;
      }

      virtual std::type_index get_type_index() const {
        return std::type_index(typeid(ForceType));
      }

      template <class T>
      inline void copy_into_impl(T *output) const {
        for (size_t i = 0; i < data_.size(); i++) {
          output[i] = (T)data_[i];
        }
      }

      virtual Semantics get_semantics() const {
        return Semantics::NUMERIC;
      }

      virtual ColHolder* combine(ColHolder *other) {
        ColHolder *retval = this;
        if (other->get_semantics() == Semantics::NUMERIC) {
          size_t sz = data_.size();
          data_.resize(sz + other->size());
          other->copy_into(data_.data() + sz);
        }
        else if (other->get_semantics() == Semantics::CATEGORICAL) {
          std::unique_ptr<ColHolder> me2(other->clone_empty());
          for (size_t i = 0; i < data_.size(); i++) {
            // The return value is not guaranteed to point to the same object as me2.
            ColHolder *me3 = me2->add_number(data_[i]);
            if (me3 != me2.get()) {
              me2.reset(me3);
            }
          }
          me2->combine(other);
          retval = me2.release();
        }
        else if (other->get_semantics() == Semantics::TEXT) {
          std::unique_ptr<TextColHolder> me2(other->clone_empty());
          for (size_t i = 0; i < data_.size(); i++) {
            // The return value is guaranteed to point to the same object as me2.
            me2->add_number(data_[i]);
          }
          me2->combine(other);
          retval = me2.release();
        }
        return retval;
      }

      virtual ColHolder* clone_empty() {
        return new NumColHolder<ForceType, ForceNumeric>(col_params_);
      }

      virtual size_t size() const {
        return data_.size();
      }

    private:
      std::vector <ForceType> data_;
      ColParams col_params_;
    };
    
    template <bool ForceNumeric>
    class NumColHolder<void, ForceNumeric> : public CRTPHolder<NumColHolder<void, ForceNumeric> > {
    public:
      NumColHolder(const ColParams &col_params) : col_params_(col_params) {}

      virtual ~NumColHolder() {}
      
      virtual ColHolder *add_string(const std::vector<char> &token) {
        data_.push_back(parse_num<float>(token.begin(), token.end()));
        return this;
      }
      
      virtual ColHolder *add_float(float val) {
        data_.push_back(val);
        return this;
      };
      
      virtual ColHolder *add_integer(long val) {
        data_.push_back(val);
        return this;
      }
      
      template <bool ForceNumeric_>
      typename std::enable_if<!ForceNumeric_, ColHolder*>::type add_string_impl(const std::vector<char> &token) {
        std::unique_ptr<ColHolder> holder(new TextColHolder());
        std::type_index idx = data_.get_type_index();
        if (idx == std::type_index(typeid(float))) {
          for (size_t i = 0; i < data_.size(); i++) {
            ColHolder *other = holder->add_string(std::to_string(data_.get<float>(i)));
            if (other != holder.get()) {
              holder.reset(other);
            }
          }
        }
        {
          ColHolder *other = holder->add_string(token);
          if (other == holder.get()) {
            holder.reset(other);
          }
        }
        return holder.release();
      }

      virtual Semantics get_semantics() const {
        return Semantics::NUMERIC;
      }

      virtual std::type_index get_type_index() const {
        return data_.get_type_index();
      }

      template <class T>
      inline void copy_into_impl(T *output) const {
        data_.copy_into(output);
      }

      virtual ColHolder* combine(ColHolder *other) {
        ColHolder *retval = this;
        if (other->get_semantics() == Semantics::NUMERIC) {
          size_t sz = data_.size();
          data_.resize(sz + other->size());
          other->copy_into(data_.data() + sz);
        }
        else if (other->get_semantics() == Semantics::CATEGORICAL) {
          std::unique_ptr<ColHolder> me2(other->clone_empty());
          for (size_t i = 0; i < data_.size(); i++) {
            // The return value is not guaranteed to point to the same object as me2.
            ColHolder *me3 = me2->add_numeric(data_[i]);
            if (me3 != me2.get()) {
              me2.release(me3);
            }
          }
          me2->combine(other);
          retval = me2->release();
        }
        else if (other->get_semantics() == Semantics::TEXT) {
          std::unique_ptr<TextColHolder> me2(other->clone_empty());
          for (size_t i = 0; i < data_.size(); i++) {
            // The return value is guaranteed to point to the same object as me2.
            me2->add_numeric(data_[i]);
          }
          me2->combine(other);
          retval = me2->release();
        }
        return retval;
      }

      virtual ColHolder* clone_empty() {
        return new NumColHolder<ForceNumeric, void>();
      }

      virtual size_t size() const {
        return data_.size();
      }

    private:
      widening_vector_dynamic<uint8_t, int8_t, int16_t, int32_t, int64_t, float> data_;
      ColParams col_params_;
    };
    
    template <bool ForceCat, bool LengthLimit, bool LevelLimit>
    class CatColHolder : public CRTPHolder<CatColHolder<ForceCat, LengthLimit, LevelLimit>> {
    public:
      CatColHolder()
        : length_limit_(std::numeric_limits<size_t>::max()),
          level_limit_(std::numeric_limits<size_t>::max()) {}

      CatColHolder(size_t length_limit, size_t level_limit, ColParams &col_params)
        : length_limit_(length_limit),
        level_limit_(level_limit),
        col_params_(col_params) {}

      virtual ~CatColHolder() {}
      
      virtual ColHolder *add_string(const std::vector<char> &token) {
        return add_string_impl(token.begin(), token.end());
      }

      virtual ColHolder *add_string(const std::string &token) {
        return add_string_impl(token.begin(), token.end());
      }

      template <class Iterator>
      typename std::enable_if<!LevelLimit && LengthLimit, ColHolder *>::type add_string_impl(Iterator begin, Iterator end) {
        ColHolder *retval = this;
        if (std::distance(begin, end) > length_limit_) {
          std::unique_ptr<ColHolder> holder(convert_to_text());
          ColHolder *other = holder->add_string(std::string(begin, end));
          if (other != holder.get()) {
            holder.reset(other);
          }
          retval = holder.release();
        }
        else {
          data_.push_back((long)get_level(begin, end));
        }
        return retval;
      }

      template <class Iterator>
      typename std::enable_if<LevelLimit && !LengthLimit, ColHolder *>::type add_string_impl(Iterator begin, Iterator end) {
        ColHolder *retval = this;
        size_t level_index = 0;
        if ((level_index = get_level(begin, end)) > level_limit_) {
          std::unique_ptr<ColHolder> holder(convert_to_text());
          holder->add_string(begin, end);
          retval = holder.release();
        }
        else {
          data_.push_back((long)level_index);
        }
        return retval;
      }

      template <class Iterator>
      typename std::enable_if<LevelLimit && LengthLimit, ColHolder *>::type add_string_impl(Iterator begin, Iterator end) {
        ColHolder *retval = this;
        size_t level_index = 0;
        if (std::distance(begin, end) > length_limit_) {
          std::unique_ptr<ColHolder> holder(convert_to_text());
          std::string token(begin, end);
          holder->add_string(token);
          retval = holder.release();
        }
        else {
          if ((level_index = get_level(begin, end)) > level_limit_) {
            std::unique_ptr<ColHolder> holder(convert_to_text());
            holder->add_string(std::string(begin, end));
            return holder;
          }
          else {
            data_.push_back((long)level_index);
          }
        }
        return this;
      }

      template <class Iterator>
      typename std::enable_if<!LevelLimit && !LengthLimit, ColHolder *>::type add_string_impl(Iterator begin, Iterator end) {
        data_.push_back(get_level(begin, end));
        return this;
      }

      template <class Iterator>
      size_t get_level(Iterator begin, Iterator end) const {
        level_.assign(begin, end);
        auto it = mapping_.find(level_);
        if (it == mapping_.end()) {
          std::tie(it, std::ignore) = mapping_.insert(std::make_pair(level_, mapping_.size()));
          levels_.push_back(level_);
        }
        return it->second;
      }

      virtual ColHolder *add_float(float val) {
        char buffer[20];
        size_t len = snprintf(buffer, 20, "%f", val);
        data_.push_back(get_level(buffer, buffer + len));
        return this;
      };

      virtual ColHolder *add_integer(long val) {
        char buffer[20];
        size_t len = snprintf(buffer, 20, "%l", val);
        data_.push_back(get_level(buffer, buffer + len));
        return this;
      }

      virtual Semantics get_semantics() const {
        return Semantics::CATEGORICAL;
      }

      virtual std::type_index get_type_index() const {
        return data_.get_type_index();
      }

      template <class T>
      inline void copy_into_impl(T *output) const {
        data_.copy_into(output);
      }

    private:
      ColHolder *convert_to_text() {
        std::unique_ptr<TextColHolder> text_holder(new TextColHolder());
        for (size_t i = 0; i < levels_.size(); i++) {
          text_holder->add_string(levels_[data_.get<long>(i)]);
        }
        return text_holder.release();
      }

      virtual ColHolder* combine(Holder *other) {
        ColHolder *retval = this;
        if (other->get_semantics() == Semantics::NUMERIC) {
          std::unique_ptr<ColHolder> A(clone_empty());
          ColHolder *B = other->combine(A);
          if (A != B->get()) {
            A.release(B);
          }
          CatColHolder *C = (CatColHolder)*B;
          size_t old_size = data_.size();
          data_.insert(data_.begin() + data_.size(), C->data_.begin(), C->data_.end());
        }
        else if (other->get_semantics() == Semantics::CATEGORICAL) {
          ColHolder *me2 = this;
          for (size_t i = 0; i < otherT->data_.size(); i++) {
            const std::string &s = data_.get_string(i);
            // The return value here is not guaranteed to be the same as this.
            me2 = me2->add_string(s.begin(), s.end());
          }
          retval = me2;
        }
        else {
          std::unique_ptr<TextColHolder> me2(other->clone_empty());
          for (size_t i = 0; i < data_.size(); i++) {
            // The return value here is guaranteed to be the same as this.
            me2->add_string(mapping_[data_[i]]);
          }
          me2->combine(other);
          retval = me2->release();          
        }
        return retval;
      }

      virtual ColHolder* clone_empty() {
        return new CatColHolder<ForceCat, LengthLimit, LevelLimit>(length_limit, level_limit);
      }

    private:
      size_t length_limit_;
      size_t level_limit_;
      ColParams col_params_;
      widening_vector_dynamic<uint8_t, uint16_t, uint32_t, uint64_t> data_;
      mutable std::string level_;
      mutable std::unordered_map<std::string, size_t> mapping_;
      mutable std::vector<std::string> levels_;
    };

    ColHolder *create_ColHolder(Semantics semantics, const ColParams &params) {
      ColHolder *retval = NULL;
      if (semantics == Semantics::NUMERIC) {
        if (params.type_hint == TH_NONE) {
          if (params.force_numeric) {
            retval = new NumColHolder<void, true> >();
          }
          else {
            retval = new NumColHolder<void, false> >();
          }
        }
        else if (params.type_hint == TH_UINT8) {
          if (params.force_numeric) {
            retval = new NumColHolder<uint8_t, true> >();
          }
          else {
            retval = new NumColHolder<uint8_t, false> >();
          }
        }
        else if (params.type_hint == TH_INT8) {
          if (params.force_numeric) {
            retval = new NumColHolder<int8_t, true> >();
          }
          else {
            retval = new NumColHolder<int8_t, false> >();
          }
        }
        else if (params.type_hint == TH_UINT16) {
          if (params.force_numeric) {
            retval = new NumColHolder<uint16_t, true> >();
          }
          else {
            retval = new NumColHolder<uint16_t, false> >();
          }
        }
        else if (params.type_hint == TH_INT16) {
          if (params.force_numeric) {
            retval = new NumColHolder<int16_t, true> >();
          }
          else {
            retval = new NumColHolder<int16_t, false> >();
          }
        }
        else if (params.type_hint == TH_UINT32) {
          if (params.force_numeric) {
            retval = new NumColHolder<uint32_t, true> >();
          }
          else {
            retval = new NumColHolder<uint32_t, false> >();
          }
        }
        else if (params.type_hint == TH_INT32) {
          if (params.force_numeric) {
            retval = new NumColHolder<int32_t, true> >();
          }
          else {
            retval = new NumColHolder<int32_t, false> >();
          }
        }
        else if (params.type_hint == TH_UINT64) {
          if (params.force_numeric) {
            retval = new NumColHolder<uint64_t, true> >();
          }
          else {
            retval = new NumColHolder<uint64_t, false> >();
          }
        }
        else if (params.type_hint == TH_INT64) {
          if (params.force_numeric) {
            retval = new NumColHolder<int64_t, true> >();
          }
          else {
            retval = new NumColHolder<int64_t, false> >();
          }
        }
      }
      else if (semantics == Semantics::CATEGORICAL) {
        if (params.max_level_name_length < std::numeric_limits<size_t>::max()
            && params.max_levels < std::numeric_limits<size_t>::max()) {
          return new CatColHolder<true, true>(params);
        }
        else if (params.max_level_name_length < std::numeric_limits<size_t>::max()) {
          return new CatColHolder<true, false>(params);
        }
        else if (params.max_levels < std::numeric_limits<size_t>::max()) {
          return new CatColHolder<false, true>(params);
        }
        else {
          return new CatColHolder<false, false>(params);
        }
      }
      else {
        return new TextColHolder();
      }
    }
  }
}
#endif
