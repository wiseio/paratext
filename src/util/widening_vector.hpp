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

#ifndef PARATEXT_WIDENING_VECTOR_HPP
#define PARATEXT_WIDENING_VECTOR_HPP

#include <vector>
#include <cstddef>
#include <limits>
#include <cstdint>
#include <cmath>

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
  static std::unordered_map<std::type_index, std::string> names;
  if (names.size() == 0) {
    names.insert(std::make_pair(std::type_index(typeid(uint8_t)), "uint8"));
    names.insert(std::make_pair(std::type_index(typeid(uint16_t)), "uint16"));
    names.insert(std::make_pair(std::type_index(typeid(uint32_t)), "uint32"));
    names.insert(std::make_pair(std::type_index(typeid(uint64_t)), "uint64"));
    names.insert(std::make_pair(std::type_index(typeid(int8_t)), "int8"));
    names.insert(std::make_pair(std::type_index(typeid(int16_t)), "int16"));
    names.insert(std::make_pair(std::type_index(typeid(int32_t)), "int32"));
    names.insert(std::make_pair(std::type_index(typeid(int64_t)), "int64"));
    names.insert(std::make_pair(std::type_index(typeid(float)), "float"));
    names.insert(std::make_pair(std::type_index(typeid(double)), "double"));
    names.insert(std::make_pair(std::type_index(typeid(std::string)), "string"));
  }
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
  static std::unordered_map<std::type_index, std::type_index> common_types;
  if (common_types.size() == 0) {
    common_types.insert(std::make_pair(std::type_index(typeid(uint8_t)),
                                       std::type_index(typeid(typename std::common_type<T, uint8_t>::type))));
    common_types.insert(std::make_pair(std::type_index(typeid(uint16_t)),
                                       std::type_index(typeid(typename std::common_type<T, uint16_t>::type))));
    common_types.insert(std::make_pair(std::type_index(typeid(uint32_t)),
                                       std::type_index(typeid(typename std::common_type<T, uint32_t>::type))));
    common_types.insert(std::make_pair(std::type_index(typeid(uint64_t)),
                                       std::type_index(typeid(typename std::common_type<T, uint64_t>::type))));
    common_types.insert(std::make_pair(std::type_index(typeid(int8_t)),
                                       std::type_index(typeid(typename std::common_type<T, int8_t>::type))));
    common_types.insert(std::make_pair(std::type_index(typeid(int16_t)),
                                       std::type_index(typeid(typename std::common_type<T, int16_t>::type))));
    common_types.insert(std::make_pair(std::type_index(typeid(int32_t)),
                                       std::type_index(typeid(typename std::common_type<T, int32_t>::type))));
    common_types.insert(std::make_pair(std::type_index(typeid(int64_t)),
                                       std::type_index(typeid(typename std::common_type<T, int64_t>::type))));
    common_types.insert(std::make_pair(std::type_index(typeid(float)),
                                       std::type_index(typeid(typename std::common_type<T, float>::type))));
    common_types.insert(std::make_pair(std::type_index(typeid(double)),
                                       std::type_index(typeid(typename std::common_type<T, double>::type))));
  }
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
  virtual widening_vector_impl_base *v_push_back(const std::string &val) {
    (void)val;
    throw std::logic_error("not implemented");
    return NULL;
  }

  virtual void v_shrink_to_fit() = 0;
  virtual widening_vector_impl_base *v_get_active() const = 0;
  virtual size_t v_size() const = 0;
  virtual float v_get_float(size_t i) const = 0;
  virtual long v_get_long(size_t i) const = 0;
  virtual void v_clear() = 0;
  virtual std::type_index v_get_type_index() const = 0;
  virtual std::type_index v_get_common_type_index(std::type_index idx) const = 0;
};

/*
 * A vector of numbers of varying precisions. Given primitive types
 * T1, T2, ..., TK, this class attempts to store the values with
 * the least precision and smallest width without losing precision.
 *
 * As more values are added, the vector widens the precision in-situ.
 */
template <size_t I, class ... Ts>
struct widening_vector_impl {};

/*
 * The inductive case assumes that given a type Head, all subsequent
 * types are at least as wide. An integral type must precede a floating
 * point type.
 */
template <size_t I, class Head, class ... Ts>
struct widening_vector_impl<I, Head, Ts...> : public widening_vector_impl_base {

  /*
    Constructs an empty widened vector.
   */
  widening_vector_impl() : active_(true) {}

  virtual ~widening_vector_impl() {}

  /*
   * Retrieves a value from the vector. If the type referred by this
   * base case type is active, it is casted to the requested type T and
   * returned. Otherwise, the request is propogated to the next type.
   */
  /*template <class T>
  T get(size_t i) const {
    if (active_) {
      return (T)values_[i];
    }
    else {
      return wider_.get<T>(i);
    }
    }*/

  /*
   * Appends this vector with another datum.
   */
  /*template <class T>
  void push_back(T value) {
    push_back_impl(value);
    }*/

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
    active_ = true;
    values_.shrink_to_fit();
  }

  virtual void v_clear() {
    clear();
  }

  virtual widening_vector_impl_base *v_get_active() const {
    return active_
      ? (widening_vector_impl_base *)this
      : (widening_vector_impl_base *)wider_.v_get_active();
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

private:

  /*
   * Widens the vector, moving the data to the next widenend vector.
   */
  void move_to_wider() {
    move_to_wider_impl<Head>();
  }

  template <class THead>
  typename std::enable_if<std::is_integral<THead>::value, void>::type move_to_wider_impl() {
    active_ = false;
    for (size_t i = 0; i < values_.size(); i++) {
      wider_.v_push_back((long)values_[i]);
    }
    values_.clear();
    values_.shrink_to_fit();
  }

  template <class THead>
  typename std::enable_if<!std::is_integral<THead>::value, void>::type move_to_wider_impl() {
    active_ = false;
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
struct widening_vector_impl<1, Head> : public widening_vector_impl_base {

  widening_vector_impl() {}

  template <class T>
  T get(size_t i) const {
    return (T)values_[i];
  }

  template <class T>
  void push_back(T value) {
    values_.push_back(value);
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

  virtual widening_vector_impl_base *v_get_active() const {
    return (widening_vector_impl_base*)this;
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

private:
  std::vector<Head> values_;
};

class widening_vector_impl_str : public widening_vector_impl_base {
public:
  widening_vector_impl_str() {}
  virtual ~widening_vector_impl_str() {}

  virtual widening_vector_impl_base *v_push_back(float val) {
    std::string s(std::to_string(val));
    push_back(s);
    return this;
  }

  virtual widening_vector_impl_base *v_push_back(long val) {
    std::string s(std::to_string(val));
    push_back(s);
    return this;
  }

  virtual widening_vector_impl_base *v_push_back(const std::string &val) {
    push_back(val);
    return this;
  }

  virtual void v_shrink_to_fit() {
    values_.shrink_to_fit();
  }

  virtual widening_vector_impl_base *v_get_active() const {
    return (widening_vector_impl_base*)this;
  }

  virtual size_t v_size() const {
    return values_.size();
  }

  virtual float v_get_float(size_t) const {
    return 0.0;
  }

  virtual long v_get_long(size_t) const {
    return 0;
  }

  virtual void v_clear() {
    values_.clear();
  }

  virtual std::type_index v_get_type_index() const {
    return std::type_index(typeid(std::string));
  }

  virtual std::type_index v_get_common_type_index(std::type_index idx) const {
    (void)idx;
    return std::type_index(typeid(std::string));
  }

private:
  void push_back(const std::string &val) {
    auto it = ids_.find(val);
    if (it != ids_.end()) {
      std::tie(it, std::ignore) = ids_.insert(std::make_pair(val, ids_.size()));
    }
    values_.push_back(it->second);
  }

private:
  std::vector<size_t> values_;
  std::unordered_map<std::string, size_t> ids_;
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
struct widening_vector_static : public widening_vector_impl<sizeof...(T), T...> {};

template <class... T>
class widening_vector_dynamic {
public:
  widening_vector_dynamic() : first_(NULL), current_(NULL), str_(NULL) {
    first_ = &values_;
    current_ = first_;
    str_ = new widening_vector_impl_str();
  }

  virtual ~widening_vector_dynamic() {
    delete str_;
    str_ = NULL;
  }
  
  void push_back(float val) {
    current_ = current_->v_push_back(val);
  }

  void push_back(long val) {
    current_ = current_->v_push_back(val);
  }

  void push_back(const std::string &val) {
    if (current_ != str_) {
      const size_t sz(current_->v_size());
      for (size_t i = 0; i < sz; i++) {
        str_->v_push_back(current_->v_get_float(i));
        current_->v_clear();
      }
      current_ = str_;
    }
    current_->v_push_back(val);    
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

private:
  widening_vector_impl<sizeof...(T), T...> values_;
  widening_vector_impl_base *first_;
  widening_vector_impl_base *current_;
  widening_vector_impl_base *str_;
};

#endif
