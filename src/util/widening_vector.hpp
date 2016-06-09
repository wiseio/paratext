/*
    ParaText: parallel text reading
    Copyright (C) 2016. wise.io, Inc.

   Licensed to the Apache Software Foundation (ASF) under one
   or more contributor license agreements.  See the NOTICE file
   distributed with this work for additional information
   regarding copyright ownership.  The ASF licenses this file
   to you under the Apache License, Version 2.0 (the
   "License"); you may not use this file except in compliance
   with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing,
   software distributed under the License is distributed on an
   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
   KIND, either express or implied.  See the License for the
   specific language governing permissions and limitations
   under the License.
 */

/*
  Coder: Damian Eads.
 */

#ifndef PARATEXT_WIDENING_VECTOR_HPP
#define PARATEXT_WIDENING_VECTOR_HPP

#include <vector>
#include <cstddef>
#include <limits>
#include <cstdint>
#include <cmath>
#include <numeric>

#include <typeinfo>
#include <typeindex>
#include <unordered_map>
#include <sstream>

template <class Head, class ... Ts>
struct contains_integral
  : public std::integral_constant<bool,
                                  contains_integral<Ts...>::value
                                  || std::is_integral<Head>::value> {};

template <class Head>
struct contains_integral<Head> : public std::is_integral<Head> {};

template <class Head, class ... Ts>
struct max_sizeof
  : public std::integral_constant<std::size_t,
                                  (sizeof(Head) > max_sizeof<Ts...>::value)
                                    ? sizeof(Head) : max_sizeof<Ts...>::value> {};

template <class Head>
struct max_sizeof<Head> : public std::integral_constant<std::size_t, sizeof(Head)> {};

static std::string get_type_name(std::type_index idx) {
  static std::unordered_map<std::type_index, std::string>
    names({std::make_pair(std::type_index(typeid(uint8_t)), "uint8"),
          std::make_pair(std::type_index(typeid(uint16_t)), "uint16"),
          std::make_pair(std::type_index(typeid(uint32_t)), "uint32"),
          std::make_pair(std::type_index(typeid(uint64_t)), "uint64"),
          std::make_pair(std::type_index(typeid(int8_t)), "int8"),
          std::make_pair(std::type_index(typeid(int16_t)), "int16"),
          std::make_pair(std::type_index(typeid(int32_t)), "int32"),
          std::make_pair(std::type_index(typeid(int64_t)), "int64"),
          std::make_pair(std::type_index(typeid(float)), "float"),
          std::make_pair(std::type_index(typeid(double)), "double"),
          std::make_pair(std::type_index(typeid(std::string)), "string")});
  auto it = names.find(idx);
  if (it == names.end()) {
    return idx.name();
  }
  else {
    return it->second;
  }
}

template <class T>
static std::type_index get_common_type_index(std::type_index idx) {
  static std::unordered_map<std::type_index, std::type_index>
    common_types({
                  std::make_pair(std::type_index(typeid(uint8_t)),
                                 std::type_index(typeid(typename std::common_type<T, uint8_t>::type))),
                  std::make_pair(std::type_index(typeid(uint16_t)),
                                 std::type_index(typeid(typename std::common_type<T, uint16_t>::type))),
                  std::make_pair(std::type_index(typeid(uint32_t)),
                                 std::type_index(typeid(typename std::common_type<T, uint32_t>::type))),
                  std::make_pair(std::type_index(typeid(uint64_t)),
                                 std::type_index(typeid(typename std::common_type<T, uint64_t>::type))),
                  std::make_pair(std::type_index(typeid(int8_t)),
                                 std::type_index(typeid(typename std::common_type<T, int8_t>::type))),
                  std::make_pair(std::type_index(typeid(int16_t)),
                                 std::type_index(typeid(typename std::common_type<T, int16_t>::type))),
                  std::make_pair(std::type_index(typeid(int32_t)),
                                 std::type_index(typeid(typename std::common_type<T, int32_t>::type))),
                  std::make_pair(std::type_index(typeid(int64_t)),
                                 std::type_index(typeid(typename std::common_type<T, int64_t>::type))),
                  std::make_pair(std::type_index(typeid(float)),
                                 std::type_index(typeid(typename std::common_type<T, float>::type))),
                  std::make_pair(std::type_index(typeid(double)),
                                 std::type_index(typeid(typename std::common_type<T, double>::type)))});
  auto it = common_types.find(idx);
  if (it == common_types.end()) {
    std::ostringstream ostr;
    ostr << "The two types ";
    ostr << std::string(get_type_name(idx));
    ostr << " and ";
    ostr << std::string(get_type_name(std::type_index(typeid(T))));
    ostr << " cannot be combined";
    throw std::logic_error(ostr.str());
  }
  return it->second;
}

struct widening_vector_impl_base {
  widening_vector_impl_base() {}
  virtual ~widening_vector_impl_base() {}

  virtual widening_vector_impl_base *v_push_back(float f) = 0;
  virtual widening_vector_impl_base *v_push_back(long f) = 0;

  virtual void v_shrink_to_fit() = 0;
  virtual size_t v_size() const = 0;
  virtual float v_get_float(size_t i) const = 0;
  virtual long v_get_long(size_t i) const = 0;
  virtual void v_clear() = 0;
  virtual std::type_index v_get_type_index() const = 0;
  virtual std::type_index v_get_common_type_index(std::type_index idx) const = 0;

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

  template <class T>
  T get_sum() const {
    return get_sum_impl(T());
  }

  virtual double get_sum_impl(double) const = 0;
  virtual float get_sum_impl(float) const = 0;
  virtual size_t get_sum_impl(size_t) const = 0;
  virtual long get_sum_impl(long) const = 0;
};

template <class WVT>
struct widening_vector_impl_crtp : public widening_vector_impl_base {
  widening_vector_impl_crtp() {}
  virtual ~widening_vector_impl_crtp() {}

  virtual void copy_into(uint8_t* array) const { ((WVT*)this)->copy_into_impl(array); }
  virtual void copy_into(uint16_t* array) const { ((WVT*)this)->copy_into_impl(array); }
  virtual void copy_into(uint32_t* array) const { ((WVT*)this)->copy_into_impl(array); }
  virtual void copy_into(uint64_t* array) const { ((WVT*)this)->copy_into_impl(array); }
  virtual void copy_into(int8_t* array) const { ((WVT*)this)->copy_into_impl(array); }
  virtual void copy_into(int16_t* array) const { ((WVT*)this)->copy_into_impl(array); }
  virtual void copy_into(int32_t* array) const { ((WVT*)this)->copy_into_impl(array); }
  virtual void copy_into(int64_t* array) const { ((WVT*)this)->copy_into_impl(array); }
  virtual void copy_into(float* array) const { ((WVT*)this)->copy_into_impl(array); }
  virtual void copy_into(double* array) const { ((WVT*)this)->copy_into_impl(array); }

  virtual double get_sum_impl(double) const { return ((WVT*)this)->template get_sum<double>(); }
  virtual float get_sum_impl(float) const { return ((WVT*)this)->template get_sum<float>(); }
  virtual size_t get_sum_impl(size_t) const { return ((WVT*)this)->template get_sum<size_t>(); }
  virtual long get_sum_impl(long) const { return ((WVT*)this)->template get_sum<long>(); }
};

/*
 * A vector of numbers of varying precisions. Given primitive types
 * T1, T2, ..., TK, this class attempts to store the values with
 * the least precision and smallest width without losing precision.
 *
 * As more values are added, the vector widens the precision in-situ.
 */
template <size_t I, class ... Ts>
struct widening_vector_impl;

/*
 * The inductive case assumes that given a type Head, all subsequent
 * types are at least as wide. An integral type must precede a floating
 * point type.
 */
template <size_t I, class Head, class ... Ts>
struct widening_vector_impl<I, Head, Ts...> : public widening_vector_impl_crtp<widening_vector_impl<I, Head, Ts...>> {

  /*
    Constructs an empty widened vector.
   */
  widening_vector_impl() {}

  virtual ~widening_vector_impl() {}

  virtual widening_vector_impl_base *v_push_back(float value) {
    //    std::cout << "f";
    return v_push_back_impl<Head>(value);
  }

  virtual widening_vector_impl_base *v_push_back(long value) {
    widening_vector_impl_base *retval = this;
    //std::cout << "i";
    if (value >= (long)std::numeric_limits<Head>::min()
        && value <= (long)std::numeric_limits<Head>::max()) {
      values_.push_back(value);
    }
    else {
      move_to_wider();
      retval = wider_.v_push_back(value);
    }
    return retval;
  }

  template <class THead>
  typename std::enable_if<std::is_floating_point<THead>::value, widening_vector_impl_base *>::type v_push_back_impl(float value) {
    widening_vector_impl_base *retval = this;
    if (value >= std::numeric_limits<THead>::min()
        && value <= std::numeric_limits<THead>::max()) {
      values_.push_back(value);
    }
    else {
      move_to_wider();
      retval = wider_.v_push_back(value);
    }
    return (widening_vector_impl_base*)retval;
  }

  template <class THead>
  typename std::enable_if<!std::is_floating_point<THead>::value, widening_vector_impl_base *>::type v_push_back_impl(float value) {
    widening_vector_impl_base *retval = this;
    if (std::trunc(value) == value
        && value >= (float)std::numeric_limits<Head>::min()
        && value <= (float)std::numeric_limits<Head>::max()) {
      values_.push_back(value);
    }
    else {
      move_to_wider();
      retval = wider_.v_push_back(value);
    }
    return (widening_vector_impl_base*)retval;
  }


  virtual void v_shrink_to_fit() {
    shrink_to_fit();
  }

  /*
   * The number of elements in the vector.
   */

  virtual size_t v_size() const {
    return values_.size();
  }

  /*
   * Shrinks the size of this vector.
   */
  void shrink_to_fit() {
    values_.shrink_to_fit();
    wider_.shrink_to_fit();
  }

  /*
   * Removes all elements from this vector.
   */
  void clear() {
    values_.clear();
    wider_.clear();
    values_.shrink_to_fit();
  }

  virtual void v_clear() {
    clear();
  }

  virtual float v_get_float(size_t i) const {
    return (float)values_[i];
  }

  virtual long v_get_long(size_t i) const {
    return (long)values_[i];
  }

  virtual std::type_index v_get_common_type_index(std::type_index idx) const {
    return get_common_type_index<Head>(idx);
  }

  virtual std::type_index v_get_type_index() const {
    return std::type_index(typeid(Head));
  }

  template <class T>
  void copy_into_impl(T *output) const {
    std::copy(values_.begin(), values_.end(), output);
  }

  template <class T>
  T get_sum() const {
    auto begin = values_.begin();
    return std::accumulate<decltype(begin)>(values_.begin(), values_.end(), (T)0);
  }

private:

  /*
   * Widens the vector, moving the data to the next widenend vector.
   */
  void move_to_wider() {
    move_to_wider_impl<Head>();
  }

  template <class THead>
  typename std::enable_if<std::is_integral<THead>::value, void>::type move_to_wider_impl() {
    for (size_t i = 0; i < values_.size(); i++) {
      wider_.v_push_back((long)values_[i]);
    }
    values_.clear();
    values_.shrink_to_fit();
  }

  template <class THead>
  typename std::enable_if<!std::is_integral<THead>::value, void>::type move_to_wider_impl() {
    for (size_t i = 0; i < values_.size(); i++) {
      wider_.v_push_back((float)values_[i]);
    }
    values_.clear();
    values_.shrink_to_fit();
  }

private:
  static_assert(std::is_floating_point<Head>::value
                || std::is_integral<Head>::value,
                "only integral or floating-point types are allowed");

  static_assert(std::is_integral<Head>::value
                || !contains_integral<Ts...>::value,
                "a floating-point type must not appear before an integral type");

private:
  bool active_;
  std::vector<Head> values_;
  widening_vector_impl<I-1, Ts...> wider_;
};

/*
 * The base case for a widening vector. The type managed by this case must
 * be the widest and most precise.
 */
template <class Head>
struct widening_vector_impl<1, Head> : public widening_vector_impl_crtp<widening_vector_impl<1, Head>> {

  widening_vector_impl() {}

  template <class T>
  inline T get_sum() const {
    auto begin = values_.begin();
    return std::accumulate<decltype(begin)>(values_.begin(), values_.end(), (T)0);
  }

  template <class T>
  T get(size_t i) const {
    return (T)values_[i];
  }

  size_t size() const {
    return values_.size();
  }

  virtual size_t v_size() const {
    return values_.size();
  }

  size_t capacity() const {
    return values_.capacity();
  }

  size_t capacity_bytes() const {
    return values_.capacity() * sizeof(Head);
  }

  void shrink_to_fit() {
    values_.shrink_to_fit();
  }

  void clear() {
    values_.clear();
  }

  virtual void v_clear() {
    values_.clear();
  }

  virtual widening_vector_impl_base *v_push_back(float val) {
    values_.push_back(val);
    return (widening_vector_impl_base*)this;
  }

  virtual widening_vector_impl_base *v_push_back(long val) {
    values_.push_back(val);
    return (widening_vector_impl_base*)this;
  }

  virtual void v_shrink_to_fit() {
    shrink_to_fit();
  }

  virtual float v_get_float(size_t i) const {
    return (float)values_[i];
  }

  virtual long v_get_long(size_t i) const {
    return (long)values_[i];
  }

  virtual std::type_index v_get_common_type_index(std::type_index idx) const {
    return get_common_type_index<Head>(idx);
  }

  virtual std::type_index v_get_type_index() const {
    return std::type_index(typeid(Head));
  }

  template <class T>
  void copy_into_impl(T *output) const {
    std::copy(values_.begin(), values_.end(), output);
  }

private:
  std::vector<Head> values_;
};

/*
 * A widenened vector, which is a vector of dynamic width numbers. In the
 * type list passed, integral types must precede floating point types.
 *
 * For example, the type::
 *
 *     widenened_vector<uint8_t, int16_t, int32_t, int64_t, float>
 *
 * is a valid widenened_vector.
 *
 * There are two version, a static version and a dynamic version. Both
 * make use of the infrastructure define in widened_vector_impl. The
 * virtual functions are used by the dynamic function and the
 * non-virtuals in widened_vector_impl are used by the static version.
 * The dynamic version is slightly easier to interact with.
 */

template <class... T>
class widening_vector_dynamic {
public:
  widening_vector_dynamic() : first_(NULL), current_(NULL) {
    first_ = &values_;
    current_ = first_;
  }

  virtual ~widening_vector_dynamic() {
  }

  void push_back(float val) {
    current_ = current_->v_push_back(val);
  }

  void push_back(long val) {
    current_ = current_->v_push_back(val);
  }

  size_t size() const {
    return current_->v_size();
  }

  template <class Q>
  typename std::enable_if<std::is_floating_point<Q>::value, Q>::type get(size_t i) const {
    return (Q)current_->v_get_float(i);
  }

  template <class Q>
  typename std::enable_if<!std::is_floating_point<Q>::value, Q>::type get(size_t i) const {
    return (Q)current_->v_get_long(i);
  }

  template <class Q>
  inline Q get_sum() const {
    return current_->get_sum<Q>();
  }

  void clear() {
    current_->v_clear();
    first_->v_clear();
    current_ = first_;
  }

  void shrink_to_fit() {
    current_->v_shrink_to_fit();
  }

  std::type_index get_type_index() const {
    return current_->v_get_type_index();
  }

  std::type_index get_common_type_index(std::type_index idx) const {
    return current_->v_get_common_type_index(idx);
  }

  virtual void copy_into(uint8_t* array) const { current_->copy_into(array); }
  virtual void copy_into(uint16_t* array) const { current_->copy_into(array); }
  virtual void copy_into(uint32_t* array) const { current_->copy_into(array); }
  virtual void copy_into(uint64_t* array) const { current_->copy_into(array); }
  virtual void copy_into(int8_t* array) const { current_->copy_into(array); }
  virtual void copy_into(int16_t* array) const { current_->copy_into(array); }
  virtual void copy_into(int32_t* array) const { current_->copy_into(array); }
  virtual void copy_into(int64_t* array) const { current_->copy_into(array); }
  virtual void copy_into(float* array) const { current_->copy_into(array); }
  virtual void copy_into(double* array) const { current_->copy_into(array); }

private:
  widening_vector_impl<sizeof...(T), T...> values_;
  widening_vector_impl_base *first_;
  widening_vector_impl_base *current_;
};

#endif
