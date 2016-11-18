#ifndef WISEIO_PYTHON_INPUT_HPP
#define WISEIO_PYTHON_INPUT_HPP

#include <Python.h>

#ifdef PARATEXT_DATE_TIME
#include <datetime.h>
#endif

#define NPY_NO_DEPRECATED_API NPY_1_7_API_VERSION
#include <numpy/arrayobject.h>
#include <numpy/npy_math.h>
#include <stdio.h>
#include <iterator>

#include "processor.hpp"
#include "util/unicode.hpp"
#include "util/strings.hpp"

namespace ParaText {
  
  class PyRefGuard {
  public:
    PyRefGuard() : object(NULL) {}
    PyRefGuard(PyObject *obj) : object(obj) {}
    
    ~PyRefGuard() {
      Py_XDECREF(object);
      object = NULL;
    }
    
    void steal(PyObject *other) {
      Py_XDECREF(object);
      object = other;
    }
    
    void disown() {
      object = NULL;
    }
    
    void decref() {
      Py_XDECREF(object);
      object = NULL;
    }
    
    PyObject *get() const {
      return object;
    }
    
  private:
    PyObject *object;
  };

  int PyBytes_GenericCheck(PyObject *obj) {
#if PY_MAJOR_VERSION >= 3
    return PyBytes_Check(obj);
#else
    return PyString_Check(obj);
#endif
  }

  std::string PyObject_ToStdString(PyObject *obj) {
    PyRefGuard guard(PyObject_Bytes(obj));
#if PY_MAJOR_VERSION >= 3
    Py_ssize_t length = PyBytes_Size(guard.get());
    char *buffer = PyBytes_AsString(guard.get());
#else
    char *buffer = PyString_AsString(guard.get());
    Py_ssize_t length = (size_t)PyString_Size(guard.get());
#endif
    return std::string(buffer, buffer + length);
  }

  void PyBytes_AsCharString(PyObject *obj, char **buffer, Py_ssize_t *length) {
#if PY_MAJOR_VERSION >= 3
    *length = PyBytes_Size(obj);
    *buffer = PyBytes_AsString(obj);
#else
    PyString_AsStringAndSize(obj, buffer, length);
#endif
  }

  std::string PyBytes_AsStdString(PyObject *obj) {
    char *buffer;
    Py_ssize_t length;
    PyBytes_AsCharString(obj, &buffer, &length);
#if PY_MAJOR_VERSION >= 3
    length = PyBytes_Size(obj);
    buffer = PyBytes_AsString(obj);
#else
    PyString_AsStringAndSize(obj, &buffer, &length);
#endif
    return std::string(buffer, buffer + length);
  }
  
  ParaText::IteratorElementType get_element_type(PyObject *obj) {
    ParaText::IteratorElementType next_type;
    if (PyBytes_GenericCheck(obj)) {
      next_type = ParaText::IteratorElementType::STRING;
    }
    else if (PyUnicode_Check(obj)) {
      next_type = ParaText::IteratorElementType::STRING;
    }
    else if (PyInt_Check(obj)) {
      next_type = ParaText::IteratorElementType::LONG;
    }
    else if (PyLong_Check(obj)) {
      next_type = ParaText::IteratorElementType::LONG;
    }
    else if (PyFloat_Check(obj)) {
      next_type = ParaText::IteratorElementType::FLOAT;
    }
    else if (PyBool_Check(obj)) {
      next_type = ParaText::IteratorElementType::BOOL;
    }
#ifdef PARATEXT_DATE_TIME
    else if (PyDateTime_Check(obj)) {
      next_type = ParaText::IteratorElementType::DATETIME;
    }
#endif
    else {
      throw std::string("cannot process type of element in sequence");
    }
    return next_type;
  }
  
  template <class T>
  typename std::enable_if<std::is_arithmetic<T>::value, float>::type
  get_as_float(const T &t, size_t item_size) {
    (void)item_size;
    return (float)t;
  }
  
  template <class T>
  typename std::enable_if<std::is_arithmetic<T>::value, float>::type
  get_as_long(const T &t, size_t item_size) {
    (void)item_size;
    return (long)t;
  }
  
  
  template <class T>
  typename std::enable_if<std::is_arithmetic<T>::value, float>::type
  get_as_bool(const T &t, size_t item_size) {
    (void)item_size;
    return (bool)t;
  }
  
  bool get_as_bool(PyObject *obj, size_t item_size) {
    (void)item_size;
    return (bool)PyObject_IsTrue(obj);
  }
  
  long get_as_long(PyObject *obj, size_t item_size) {
    (void)item_size;
    long retval = 0;
    if (PyInt_Check(obj)) {
      retval = PyInt_AsLong(obj);
    }
    else if (PyLong_Check(obj)) {
      retval = PyLong_AsLong(obj);
    }
    else if (PyBool_Check(obj)) {
      retval = PyObject_IsTrue(obj);
    }
    else if (PyFloat_Check(obj)) {
      retval = (long)PyFloat_AsDouble(obj);
    }
    else if (PyBytes_GenericCheck(obj)) {
      char *buf;
      Py_ssize_t length;
      PyBytes_AsCharString(obj, &buf, &length);
      retval = ::fast_atoi<long>(buf, buf + length);
    }
    else if (PyUnicode_Check(obj)) {
      PyRefGuard temp(PyUnicode_AsUTF8String(obj));
      if (temp.get() == NULL) {
        throw std::string("unable to convert unicode string to UTF-8");
      }
      Py_ssize_t size;
      const char *buf = PyUnicode_AS_DATA(temp.get());
      size = PyUnicode_GET_DATA_SIZE(temp.get());
      retval = ::fast_atoi<long>(buf, buf + size);
    }
    else {
      throw std::string("cannot handle item");
    }
    return retval;
  }
  
  std::string get_as_string(const std::string &s, size_t) {
    return s;
  }
  
  template <class T>
  typename std::enable_if<std::is_arithmetic<T>::value, std::string>::type
  get_as_string(const T &val, size_t) {
    std::ostringstream ostr;
    ostr << val;
    return ostr.str();
  }
  
  std::string get_as_string(const npy_ucs4 *buf, size_t item_size) {
    const npy_ucs4 *begin = buf;
    const npy_ucs4 *end = buf + item_size / 4;
    std::string retval;
    WiseIO::convert_utf32_to_utf8(begin, end, std::back_inserter(retval), false);
    return retval;
  }
  
  std::string get_as_string(const char *buf, size_t item_size) {
    const char *begin = buf;
    const char *end = buf + item_size;
    std::string retval(begin, end);
    return retval;
  }
    
  std::string get_as_string(PyObject *obj, size_t) {
    if (PyBytes_GenericCheck(obj)) {
      return PyBytes_AsStdString(obj);
    }
    else if (PyUnicode_Check(obj)) {
      PyRefGuard temp(PyUnicode_AsUTF8String(obj));
      if (temp.get() == NULL) {
        throw std::string("unable to convert unicode string to UTF-8");
      }
      const char *buf = PyBytes_AsString(temp.get());
      size_t size = (size_t)PyBytes_Size(temp.get());
      std::string s(buf, buf + size);
      return s;
    }
    else if (PyFloat_Check(obj)) {
      std::ostringstream ostr;
      double dval = PyFloat_AsDouble(obj);
      ostr << dval;
      return ostr.str();
    }
    else if (PyInt_Check(obj)) {
      std::ostringstream ostr;
      long ival = PyInt_AsLong(obj);
      ostr << ival;
      return ostr.str();
    }
    else if (PyLong_Check(obj)) {
      std::ostringstream ostr;
      long ival = PyLong_AsLong(obj);
      ostr << ival;
      return ostr.str();
    }
    else if (PyBool_Check(obj)) {
      std::ostringstream ostr;
      bool bval = PyObject_IsTrue(obj);
      ostr << bval;
      return ostr.str();
    }
    else {
      return PyObject_ToStdString(obj);
    }
  }
  
  double get_as_float(const npy_ucs4 *buf, size_t item_size) {
    (void)buf;
    std::string s(get_as_string(buf, item_size));
    return ::bsd_strtod(s.begin(), s.end());
  }
  
  double get_as_float(const char *buf, size_t item_size) {
    (void)buf;
    std::string s(get_as_string(buf, item_size));
    return ::bsd_strtod(s.begin(), s.end());
  }
  
  long get_as_long(const npy_ucs4 *buf, size_t item_size) {
    (void)buf;
    std::string s(get_as_string(buf, item_size));
    return ::fast_atoi<long>(s.begin(), s.end());
  }
  
  long get_as_long(const char *buf, size_t item_size) {
    (void)buf;
    std::string s(get_as_string(buf, item_size));
    return ::fast_atoi<long>(s.begin(), s.end());
  }
  
  long get_as_bool(npy_ucs4 *buf, size_t item_size) {
    (void)buf;
    (void)item_size;
    long retval = 0;
    throw std::string("cannot convert UCS-4 to a float");
    return retval;
  }
  
  long get_as_bool(char *buf, size_t item_size) {
    (void)buf;
    (void)item_size;
    long retval = 0;
    throw std::string("cannot convert UTF-8 to a float");
    return retval;
  }

  double get_as_float(PyObject *obj, size_t item_size) {
    (void)item_size;
    double retval = 0;
    if (PyFloat_Check(obj)) {
      retval = PyFloat_AsDouble(obj);
    }
    else if (PyInt_Check(obj)) {
      retval = PyInt_AsLong(obj);
    }
    else if (PyLong_Check(obj)) {
      retval = PyLong_AsLong(obj);
    }
    else if (PyBool_Check(obj)) {
      retval = PyObject_IsTrue(obj);
    }
    else if (PyBytes_GenericCheck(obj)) {
      char *buf;
      Py_ssize_t length;
      PyBytes_AsCharString(obj, &buf, &length);
      retval = ::bsd_strtod(buf, buf + length);
    }
    else if (PyUnicode_Check(obj)) {
      PyRefGuard temp(PyUnicode_AsUTF8String(obj));
      if (temp.get() == NULL) {
        throw std::string("unable to convert unicode string to UTF-8");
      }
      char *buf = PyString_AsString(temp.get());
      auto size = PyUnicode_GET_DATA_SIZE(temp.get());
      retval = ::bsd_strtod(buf, buf + size);
    }
    else {
      throw std::string("cannot handle item");
    }
    return retval;
  }
  
  ParaText::IteratorElementType get_element_type(char *buf) {
    (void)buf;
    return ParaText::IteratorElementType::STRING;
  }
  
  ParaText::IteratorElementType get_element_type(npy_ucs4 *buf) {
    (void)buf;
    return ParaText::IteratorElementType::STRING;
  }
  
  template <class T>
  typename std::enable_if<std::is_floating_point<T>::value, ParaText::IteratorElementType>::type
  get_element_type(T) {
    return ParaText::IteratorElementType::FLOAT;
  }
  
  template <class T>
  typename std::enable_if<std::is_integral<T>::value && !std::is_same<T, bool>::value, ParaText::IteratorElementType>::type
    get_element_type(T) {
    return ParaText::IteratorElementType::LONG;
  }
  
  template <class T>
  typename std::enable_if<std::is_integral<T>::value && std::is_same<T, bool>::value, ParaText::IteratorElementType>::type
    get_element_type(T) {
    return ParaText::IteratorElementType::BOOL;
  }

#ifdef PARATEXT_DATE_TIME

  template <class T>
  typename std::enable_if<std::is_arithmetic<T>::value, boost::posix_time::ptime>::type
  get_as_datetime(T, size_t) {
    throw std::logic_error("cannot convert item to datetime");
  }

  boost::posix_time::ptime get_as_datetime(const char *buf, size_t item_size) {
    std::string s(get_as_string(buf, item_size));
    return boost::posix_time::time_from_string(s);
  }

  boost::posix_time::ptime get_as_datetime(const npy_ucs4 *buf, size_t item_size) {
    std::string s(get_as_string(buf, item_size));
    return boost::posix_time::time_from_string(s);
  }

  boost::posix_time::ptime get_as_datetime(PyObject *obj, size_t item_size) {
    (void)item_size;
    if (PyDateTime_Check(obj)) {
      int year = PyDateTime_GET_YEAR(obj);
      int month = PyDateTime_GET_MONTH(obj);
      int day = PyDateTime_GET_DAY(obj);
      int hour = PyDateTime_DATE_GET_HOUR(obj);
      int mins = PyDateTime_DATE_GET_MINUTE(obj);
      int secs = PyDateTime_DATE_GET_SECOND(obj);
      int ms = PyDateTime_DATE_GET_MICROSECOND(obj);
      boost::posix_time::ptime t(boost::gregorian::date(year, month, day),
                                 boost::posix_time::time_duration(hour, mins, secs, ms));
      return t;
    }
    else if (PyUnicode_Check(obj)) {
      Py_ssize_t size;
      PyRefGuard temp(PyUnicode_AsUTF8String(obj));
      const char *buf = PyUnicode_AS_DATA(temp.get());
      size = PyUnicode_GET_DATA_SIZE(temp.get());
      return get_as_datetime(buf, size);
    }
    else if (PyBytes_GenericCheck(obj)) {
      const char *buf;
      Py_ssize_t length;
      PyBytes_AsCharString(obj, &buf, &length);
      return get_as_datetime(buf, length);
    }
    else {
      throw std::logic_error("cannot convert item to string");
    }
  }

#endif
  
  class PythonSequenceAdvancer {
  public:
    PythonSequenceAdvancer(PyObject *sequence) : idx_(0), size_(0), sequence_(sequence) {
      size_ = PySequence_Size(sequence);
      Py_XINCREF((PyObject*)sequence);
    }
    
    ~PythonSequenceAdvancer() {
      Py_XDECREF((PyObject*)sequence_);
      sequence_ = NULL;
    }
    
    bool has_next() const {
      return idx_ < size_;
    }
    
    void advance() {
      PyObject *next_item = NULL;
      if (idx_ < size_) {
        next_item = PySequence_GetItem(sequence_, idx_);
        guard_.steal(next_item);
      }
      else {
        guard_.decref();
      }
      idx_++;
    }
    
    PyObject *current() const {
      return guard_.get();
    }
    
    size_t get_item_size() const {
      return sizeof(PyObject*);
    }
    
    size_t size() const {
      return size_;
    }
    
  private:
    Py_ssize_t idx_;
    Py_ssize_t size_;
    PyObject *sequence_;
    PyRefGuard guard_;
  };
  
  class PythonIteratorAdvancer {
  public:
    PythonIteratorAdvancer(PyObject *sequence) : size_(0), sequence_(sequence) {
      size_ = std::numeric_limits<size_t>::max();
      PyObject *next_item = PyIter_Next(sequence_);
      next_guard_.steal(next_item);
      Py_XINCREF((PyObject*)sequence);
    }
    
    ~PythonIteratorAdvancer() {
      Py_XDECREF((PyObject*)sequence_);
      sequence_ = NULL;
    }
    
    bool has_next() const {
      return next_guard_.get() != NULL;
    }
    
    void advance() {
      PyObject *item = PyIter_Next(sequence_);
      PyObject *new_cur = next_guard_.get();
      next_guard_.disown();
      next_guard_.steal(item);
      cur_guard_.steal(new_cur);
    }
    
    PyObject *current() const {
      return cur_guard_.get();
    }
    
    size_t get_item_size() const {
      return sizeof(PyObject*);
    }
    
    size_t size() const {
      return std::numeric_limits<size_t>::max();
    }
    
    Py_ssize_t size_;
    PyObject *sequence_;
    PyRefGuard next_guard_;
    PyRefGuard cur_guard_;
  };
  
  template <class npy_prim>
  class NumPyAdvancer {
  public:
    
    NumPyAdvancer() : idx_(0), size_(0), sequence_(NULL) {}
    
    NumPyAdvancer(PyArrayObject *sequence) : idx_(0), size_(0), sequence_(sequence) {
      if (PyArray_NDIM(sequence) != 1) {
        throw std::string("a NumPy array must be 1-dimensional. use reshape");
      }
      size_ = PyArray_DIM(sequence, 0);
      item_ = (npy_prim)0;
      item_size_ = PyArray_ITEMSIZE(sequence_);
      Py_XINCREF((PyObject*)sequence);
    }
    
    ~NumPyAdvancer() {
      Py_XDECREF((PyObject*)sequence_);
      sequence_ = NULL;
    }
    
    inline bool has_next() const {
      return idx_ < size_;
    }
    
    inline void advance() {
      advance_impl<npy_prim>();
    }

    template <class Prim>
    inline typename std::enable_if<std::is_same<Prim, npy_ucs4*>::value
                                   || std::is_same<Prim, char*>::value, void>::type advance_impl() {
      item_ = (npy_prim)PyArray_GETPTR1(sequence_, idx_);
      idx_++;      
    }

    template <class Prim>
    inline typename std::enable_if<std::is_arithmetic<Prim>::value, void>::type advance_impl() {
      item_ = *(npy_prim*)PyArray_GETPTR1(sequence_, idx_);
      idx_++;
    }

    template <class Prim>
    inline typename std::enable_if<std::is_same<Prim, PyObject*>::value
                                   || std::is_same<Prim, PyObject*>::value, void>::type advance_impl() {
      /* Returns PyObject** */
      item_ = *(npy_prim*)PyArray_GETPTR1(sequence_, idx_);
      idx_++;      
    }
    
    inline npy_prim current() const {
      return item_;
    }
    
    inline size_t get_item_size() const {
      return item_size_;
    }
    
    inline size_t size() const {
      return size_;
    }
    
    npy_intp idx_;
    npy_intp size_;
    npy_intp item_size_;
    PyArrayObject *sequence_;
    PyRefGuard guard_;
    npy_prim item_;
  };


template <class Advancer>
class PythonIteratorProcessor : public ParaText::IteratorProcessor {
public:
  PythonIteratorProcessor() {}

  PythonIteratorProcessor(std::shared_ptr<Advancer> advancer) : advancer(advancer) {}

  virtual ~PythonIteratorProcessor() {}

  virtual ParaText::IteratorElementType get_type() const {
    return get_element_type(advancer->current());
  }

  virtual std::string get_string() const {
    return get_as_string(advancer->current(), advancer->get_item_size());
  }

  virtual double get_float() const {
    return get_as_float(advancer->current(), advancer->get_item_size());
  }

  virtual long get_long() const {
    return get_as_long(advancer->current(), advancer->get_item_size());
  }

  virtual bool get_bool() const {
    return get_as_bool(advancer->current(), advancer->get_item_size());
  }

#ifdef PARATEXT_DATE_TIME
  virtual boost::posix_time::ptime get_datetime() const {
    return get_as_datetime(advancer->current(), advancer->get_item_size());
  }
#endif

  virtual void advance() {
    advancer->advance();
  }

  virtual size_t size() const {
    return advancer->size();
  }

  virtual bool has_next() const {
    return advancer->has_next();
  }

private:
  PyObject *obj;
  std::shared_ptr<Advancer> advancer;
};

std::shared_ptr<ParaText::IteratorProcessor> get_python_iterator_processor(PyObject *object) {
  std::shared_ptr<ParaText::IteratorProcessor> retval;
  PyObject *iter = NULL;
  if (PyArray_Check(object)) {
    PyArrayObject *array = (PyArrayObject*)object;
    npy_int ndim = PyArray_NDIM(array);
    if (ndim != 1) {
      throw std::logic_error("can only process 1D arrays");
      retval.reset();
    }
    switch (PyArray_TYPE(array)) {
    case NPY_OBJECT:
      {
        std::shared_ptr<NumPyAdvancer<PyObject*> > advancer(std::make_shared<NumPyAdvancer<PyObject*> >(array));
        auto ptr = std::make_shared<PythonIteratorProcessor<NumPyAdvancer<PyObject*> > >(advancer);
        retval = std::static_pointer_cast<ParaText::IteratorProcessor>(ptr);
      }
      break;
    case NPY_STRING:
      {
        std::shared_ptr<NumPyAdvancer<char*> > advancer(std::make_shared<NumPyAdvancer<char*> >(array));
        auto ptr = std::make_shared<PythonIteratorProcessor<NumPyAdvancer<char*> > >(advancer);
        retval = std::static_pointer_cast<ParaText::IteratorProcessor>(ptr);
      }
      break;
    case NPY_UNICODE:
      {
        std::shared_ptr<NumPyAdvancer<npy_ucs4*> > advancer(std::make_shared<NumPyAdvancer<npy_ucs4*> >(array));
        auto ptr = std::make_shared<PythonIteratorProcessor<NumPyAdvancer<npy_ucs4*> > >(advancer);
        retval = std::static_pointer_cast<ParaText::IteratorProcessor>(ptr);
      }
      break;
    case NPY_FLOAT:
      {
        std::shared_ptr<NumPyAdvancer<npy_float> > advancer(std::make_shared<NumPyAdvancer<npy_float> >(array));
        auto ptr = std::make_shared<PythonIteratorProcessor<NumPyAdvancer<npy_float> > >(advancer);
        retval = std::static_pointer_cast<ParaText::IteratorProcessor>(ptr);
      }
      break;
    case NPY_DOUBLE:
      {
        std::shared_ptr<NumPyAdvancer<npy_double> > advancer(std::make_shared<NumPyAdvancer<npy_double> >(array));
        auto ptr = std::make_shared<PythonIteratorProcessor<NumPyAdvancer<npy_double> > >(advancer);
        retval = std::static_pointer_cast<ParaText::IteratorProcessor>(ptr);
      }
      break;
    case NPY_UINT8:
      {
        std::shared_ptr<NumPyAdvancer<npy_uint8> > advancer(std::make_shared<NumPyAdvancer<npy_uint8> >(array));
        auto ptr = std::make_shared<PythonIteratorProcessor<NumPyAdvancer<npy_uint8> > >(advancer);
        retval = std::static_pointer_cast<ParaText::IteratorProcessor>(ptr);
      }
      break;
    case NPY_INT8:
      {
        std::shared_ptr<NumPyAdvancer<npy_int8> > advancer(std::make_shared<NumPyAdvancer<npy_int8> >(array));
        auto ptr = std::make_shared<PythonIteratorProcessor<NumPyAdvancer<npy_int8> > >(advancer);
        retval = std::static_pointer_cast<ParaText::IteratorProcessor>(ptr);
      }
      break;
#ifdef NPY_UINT16
    case NPY_UINT16:
      {
        std::shared_ptr<NumPyAdvancer<npy_uint16> > advancer(std::make_shared<NumPyAdvancer<npy_uint16> >(array));
        auto ptr = std::make_shared<PythonIteratorProcessor<NumPyAdvancer<npy_uint16> > >(advancer);
        retval = std::static_pointer_cast<ParaText::IteratorProcessor>(ptr);
      }
      break;
#endif
#ifdef NPY_INT16
    case NPY_INT16:
      {
        std::shared_ptr<NumPyAdvancer<npy_int16> > advancer(std::make_shared<NumPyAdvancer<npy_int16> >(array));
        auto ptr = std::make_shared<PythonIteratorProcessor<NumPyAdvancer<npy_int16> > >(advancer);
        retval = std::static_pointer_cast<ParaText::IteratorProcessor>(ptr);
      }
      break;
#endif
#ifdef NPY_UINT32
    case NPY_UINT32:
      {
        std::shared_ptr<NumPyAdvancer<npy_uint32> > advancer(std::make_shared<NumPyAdvancer<npy_uint32> >(array));
        auto ptr = std::make_shared<PythonIteratorProcessor<NumPyAdvancer<npy_uint32> > >(advancer);
        retval = std::static_pointer_cast<ParaText::IteratorProcessor>(ptr);
      }
      break;
#endif
#ifdef NPY_INT32
    case NPY_INT32:
      {
        std::shared_ptr<NumPyAdvancer<npy_int32> > advancer(std::make_shared<NumPyAdvancer<npy_int32> >(array));
        auto ptr = std::make_shared<PythonIteratorProcessor<NumPyAdvancer<npy_int32> > >(advancer);
        retval = std::static_pointer_cast<ParaText::IteratorProcessor>(ptr);
      }
      break;
#endif
    case NPY_UINT64:
      {
        std::shared_ptr<NumPyAdvancer<npy_uint64> > advancer(std::make_shared<NumPyAdvancer<npy_uint64> >(array));
        auto ptr = std::make_shared<PythonIteratorProcessor<NumPyAdvancer<npy_uint64> > >(advancer);
        retval = std::static_pointer_cast<ParaText::IteratorProcessor>(ptr);
      }
      break;
    case NPY_INT64:
      {
        std::shared_ptr<NumPyAdvancer<npy_int64> > advancer(std::make_shared<NumPyAdvancer<npy_int64> >(array));
        auto ptr = std::make_shared<PythonIteratorProcessor<NumPyAdvancer<npy_int64> > >(advancer);
        retval = std::static_pointer_cast<ParaText::IteratorProcessor>(ptr);
      }
      break;
#ifdef NPY_BOOL
    case NPY_BOOL:
      {
        std::shared_ptr<NumPyAdvancer<npy_bool> > advancer(std::make_shared<NumPyAdvancer<npy_bool> >(array));
        auto ptr = std::make_shared<PythonIteratorProcessor<NumPyAdvancer<npy_bool> > >(advancer);
        retval = std::static_pointer_cast<ParaText::IteratorProcessor>(ptr);
      }
      break;
#endif
    default:
      throw std::logic_error("cannot process dtype");
      retval = NULL;
    }
  }
  else if (PySequence_Check(object)) {
    std::shared_ptr<PythonSequenceAdvancer> advancer(std::make_shared<PythonSequenceAdvancer>(object));
    auto ptr = std::make_shared<PythonIteratorProcessor<PythonSequenceAdvancer> >(advancer);
    retval = std::static_pointer_cast<ParaText::IteratorProcessor>(ptr);
  }
  else if (PyIter_Check(object)) {
    std::shared_ptr<PythonIteratorAdvancer> advancer(std::make_shared<PythonIteratorAdvancer>(object));
    auto ptr = std::make_shared<PythonIteratorProcessor<PythonIteratorAdvancer> >(advancer);
    retval = std::static_pointer_cast<ParaText::IteratorProcessor>(ptr);
  }
  else if ((iter = PyObject_GetIter(object)) != NULL) {
    PyRefGuard guard(iter);
    std::shared_ptr<PythonIteratorAdvancer> advancer(std::make_shared<PythonIteratorAdvancer>(iter));
    auto ptr = std::make_shared<PythonIteratorProcessor<PythonIteratorAdvancer> >(advancer);
    retval = std::static_pointer_cast<ParaText::IteratorProcessor>(ptr);
  }
  else {
    throw std::logic_error("cannot process object passed - not a sequence, an iterator, an iterable, or an array");
    retval.reset();
  }
  return retval;
}
}
#endif
