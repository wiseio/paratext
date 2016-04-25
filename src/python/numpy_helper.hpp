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

#ifndef WISEIO_NUMPY_HELPER_HPP
#define WISEIO_NUMPY_HELPER_HPP

#include <unordered_map>
#include <typeinfo>
#include <typeindex>
#include <memory>
#include <iostream>

template <class T>
struct numpy_type {};

template <> struct numpy_type<uint8_t>  { static const long id = NPY_UINT8; };
template <> struct numpy_type<int8_t>   { static const long id = NPY_INT8; };
template <> struct numpy_type<uint16_t> { static const long id = NPY_UINT16; };
template <> struct numpy_type<int16_t>  { static const long id = NPY_INT16; };
template <> struct numpy_type<uint32_t> { static const long id = NPY_UINT32; };
template <> struct numpy_type<int32_t>  { static const long id = NPY_INT32; };
template <> struct numpy_type<uint64_t> { static const long id = NPY_UINT64; };
template <> struct numpy_type<int64_t>  { static const long id = NPY_INT64; };
template <> struct numpy_type<float> { static const long id = NPY_FLOAT; };
template <> struct numpy_type<double>  { static const long id = NPY_DOUBLE; };
template <> struct numpy_type<std::string>  { static const long id = NPY_OBJECT; };

template <class Container, class Enable=void>
struct build_array_impl {};

template <class Container>
struct build_array_impl<Container, typename std::enable_if<std::is_arithmetic<typename Container::value_type>::value>::type> {
  typedef typename Container::value_type value_type;

  static PyObject *build_array(const Container &container) {
    npy_intp fdims[] = {(npy_intp)container.size()};
    PyObject *obj = (PyObject*)PyArray_SimpleNew(1, fdims, numpy_type<value_type>::id);
    try {
      value_type *data = (value_type*)PyArray_DATA((PyArrayObject*)obj);
      for (size_t i = 0; i < container.size(); i++) {
        data[i] = container[i];
      }
    }
    catch (...) {
      Py_XDECREF(obj);
      obj = NULL;
      std::rethrow_exception(std::current_exception());
    }
    return obj;
  }

};

template <class Container>
struct build_array_impl<Container, typename std::enable_if<std::is_same<typename Container::value_type, std::string>::value>::type> {

  typedef typename Container::value_type value_type;

  static PyObject *build_array(const Container &container) {
    size_t sz = (size_t)container.size();
    npy_intp fdims[] = {(npy_intp)sz};
    PyObject *obj = (PyObject*)PyArray_SimpleNew(1, fdims, NPY_OBJECT);
    try { 
      for (size_t i = 0; i < container.size(); i++) {
        PyObject **ref = (PyObject **)PyArray_GETPTR1((PyArrayObject*)obj, i);
        PyObject *newobj = PyString_FromStringAndSize(container[i].c_str(), container[i].size());
        Py_XDECREF(*ref);
        *ref = newobj;
        Py_XINCREF(*ref);
      }
    }
    catch (...) {
      for (size_t i = 0; i < sz; i++) {
        PyObject **ref = (PyObject **)PyArray_GETPTR1((PyArrayObject*)obj, i);
        Py_XDECREF(*ref);
        *ref = Py_None;
        Py_XINCREF(*ref);
      }
      Py_XDECREF(obj);
      std::rethrow_exception(std::current_exception());      
    }
    return obj;
  }

};


template <class Iterator, class Enable=void>
struct build_array_from_range_impl {};

template <class Iterator>
struct build_array_from_range_impl<Iterator, typename std::enable_if<std::is_arithmetic<typename std::iterator_traits<Iterator>::value_type>::value>::type> {
  typedef typename Iterator::value_type value_type;

  static PyObject *build_array(const std::pair<Iterator, Iterator> &range) {
    npy_intp fdims[] = {(npy_intp)std::distance(range.first, range.second)};
    PyObject *obj = (PyObject*)PyArray_SimpleNew(1, fdims, numpy_type<value_type>::id);
    value_type *data = (value_type*)PyArray_DATA((PyArrayObject*)obj);
    size_t i = 0;
    for (Iterator it = range.first; it != range.second; it++, i++) {
      data[i] = *it;
    }
    return obj;
  }
};

template <class Iterator>
struct build_array_from_range_impl<Iterator, typename std::enable_if<std::is_same<typename std::iterator_traits<Iterator>::value_type, std::string>::value>::type> {

  typedef typename Iterator::value_type value_type;

  static PyObject *build_array(const std::pair<Iterator, Iterator> &range) {
    size_t sz = (npy_intp)std::distance(range.first, range.second);
    npy_intp fdims[] = {(npy_intp)sz};
    PyObject *obj = (PyObject*)PyArray_SimpleNew(1, fdims, NPY_OBJECT);
    try {
      size_t i = 0;
      for (Iterator it = range.first; it != range.second; it++, i++) {
        PyObject **ref = (PyObject **)PyArray_GETPTR1((PyArrayObject*)obj, i);
        PyObject *newobj = PyString_FromStringAndSize((*it).c_str(), (*it).size());
        Py_XDECREF(*ref);
        *ref = newobj;
        Py_XINCREF(*ref);
      }
    }
    catch (...) {
      size_t i = 0;
      for (i = 0; i < sz; i++) {
        PyObject **ref = (PyObject **)PyArray_GETPTR1((PyArrayObject*)obj, i);
        Py_XDECREF(*ref);
        *ref = Py_None;
        Py_XINCREF(*ref);
      }
      std::rethrow_exception(std::current_exception());
    }
    return obj;
  }
};

template <class Populator>
struct base_insert_populator_impl {
  base_insert_populator_impl() {}
  virtual ~base_insert_populator_impl() {}

  virtual PyObject *populate(const Populator &populator) = 0;
};

template <class Populator, class T>
struct derived_insert_populator_impl : public base_insert_populator_impl<Populator> {
  typedef T value_type;

  derived_insert_populator_impl() {}
  virtual ~derived_insert_populator_impl() {}

  virtual PyObject *populate(const Populator &populator) {
    npy_intp fdims[] = {(npy_intp)populator.size()};
    PyObject *obj = NULL;
    try {
      obj = (PyObject*)PyArray_SimpleNew(1, fdims, numpy_type<value_type>::id);
      value_type *data = (value_type*)PyArray_DATA((PyArrayObject*)obj);
      populator.insert(data);
    }
    catch (...) {
      std::cerr << "^^^B";
      std::rethrow_exception(std::current_exception());
    }
    return obj;
  }
};

template <class Populator>
PyObject *build_populator(const Populator &populator) {
  static std::unordered_map<std::type_index, std::shared_ptr<base_insert_populator_impl<Populator>>> populators;
  if (populators.size() == 0) {
    populators.insert(std::make_pair(std::type_index(typeid(uint8_t)),
                              std::make_shared<derived_insert_populator_impl<Populator, uint8_t>>()));
    populators.insert(std::make_pair(std::type_index(typeid(int8_t)),
                              std::make_shared<derived_insert_populator_impl<Populator, int8_t>>()));

    populators.insert(std::make_pair(std::type_index(typeid(uint16_t)),
                              std::make_shared<derived_insert_populator_impl<Populator, uint16_t>>()));
    populators.insert(std::make_pair(std::type_index(typeid(int16_t)),
                              std::make_shared<derived_insert_populator_impl<Populator, int16_t>>()));

    populators.insert(std::make_pair(std::type_index(typeid(uint32_t)),
                              std::make_shared<derived_insert_populator_impl<Populator, uint32_t>>()));
    populators.insert(std::make_pair(std::type_index(typeid(int32_t)),
                              std::make_shared<derived_insert_populator_impl<Populator, int32_t>>()));

    populators.insert(std::make_pair(std::type_index(typeid(uint64_t)),
                              std::make_shared<derived_insert_populator_impl<Populator, uint64_t>>()));
    populators.insert(std::make_pair(std::type_index(typeid(int64_t)),
                              std::make_shared<derived_insert_populator_impl<Populator, int64_t>>()));

    populators.insert(std::make_pair(std::type_index(typeid(float)),
                              std::make_shared<derived_insert_populator_impl<Populator, float>>()));
    populators.insert(std::make_pair(std::type_index(typeid(double)),
                              std::make_shared<derived_insert_populator_impl<Populator, double>>()));
  }
  auto it = populators.find(populator.get_type_index());
  if (it == populators.end()) {
    throw std::logic_error(std::string("cannot process type"));
  }
  return it->second->populate(populator);
}

template <class Container>
PyObject *build_array(const Container &container) {
  return (PyObject*)build_array_impl<Container>::build_array(container);
}

template <class Iterator>
PyObject *build_array_from_range(const std::pair<Iterator, Iterator> &range) {
  return (PyObject*)build_array_from_range_impl<Iterator>::build_array(range);
}

#endif
