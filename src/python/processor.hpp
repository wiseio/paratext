/*
  File: processor.bhpp

  Author: Damian Eads, PhD

  Copyright (C) wise.io, Inc. 2015.
 */

#ifndef WISEIO_PROCESSOR_HPP
#define WISEIO_PROCESSOR_HPP

#include <string>
#include <exception>

#ifdef PARATEXT_DATE_TIME
#include <boost/date_time/posix_time/posix_time.hpp>
#endif

namespace ParaText {

  /*
    A generic call-back interface for processing a sequence of
    variably-typed objects coming from a different language.

    For example, if a C++ functor requires a sequence, it
    can implement the interface of this class.

    WiseTransfer will iterate over the array, list, tuple,
    iterable object, or sequence object. When an element of a
    string is found, it calls process_string. If it is floating
    point, process_float is called. If it is a long integer,
    process_long is called.
   */
  class CallbackProcessor {
  public:

    /*
      The base constructor. Does nothing in this part of sub-object
      construction.
     */
    CallbackProcessor();

    /*
      The destructor deletes this callback processor and deallocates
      any temporary resources needed.
     */
    virtual ~CallbackProcessor();
    
    /*
      Tells the functor to ingest the next element, which is a string.
     */
    virtual void process_string(const char *begin, const char *end) = 0;

    /*
      Tells the functor to ingest the next element, which is a float.
     */
    virtual void process_float(float fval) = 0;

    /*
      Tells the functor to ingest the next element, which is a long.
     */
    virtual void process_long(long long lval) = 0;

    /*
      Tells the functor to ingest the next element, which is a bool.
     */
    virtual void process_bool(bool bval) = 0;

    /*
      Asks the functor to translate an exception thrown while calling
      one of the process_XXX methods into a string.
     */
    virtual void process_exception(std::exception_ptr ptr, std::string &text) = 0;

    /*
      Process the next sparse value.
     */
    virtual void process_sparse(size_t row_index, size_t col_index, float value) = 0;


    /*
      Process an empty sparse row.
     */
    virtual void process_sparse(size_t row_index) = 0;
  };

  /*
    An enumerated type for identifying the type of element in an
    IteratorProcessor.
   */
  enum class IteratorElementType {STRING, LONG, BOOL, FLOAT, DATETIME};

  /*
    An IteratorProcessor (iterproc for short) generic interface for
    manipulating an iterator over primitive types in another language.

    An iterproc X can be queried if there are any more elements remaining
    as follows::

        while (X.has_next()) {
           switch (X.get_type()) {
           case IteratorElementType::STRING:
              ...
              break;
           }
           X.advance();
        }

    The get_type() function returns the type of the current element.
    The advance() function advances the iterator to the next element.
    The element that the iterator is currently pointing to can be
    retrieved with:

          X.get_string()
          X.get_float()
          X.get_long()
          X.get_bool()

    
   */
  class IteratorProcessor {
  public:
    /*
      The base constructor for an IteratorProcessor. This part of
      the sub-object construction does nothing.
     */
    IteratorProcessor() {}

    /*
      A virtual destructor for the iterator processor.
    */
    virtual ~IteratorProcessor() {}

    /*
      The type of the element to which the iterator currently points.
    */
    virtual IteratorElementType get_type() const = 0;

    /*
      Retrieves a string representation of the current element.
     */
    virtual std::string get_string() const = 0;

    /*
      Retrieves a float at the current element.
     */
    virtual double get_float() const = 0;

    /*
      Retrieves a long at the current element.
     */
    virtual long long get_long() const = 0;

#ifdef PARATEXT_DATE_TIME
    /*
      Retrieves a date time at the current element.
     */
    virtual boost::posix_time::ptime get_datetime() const = 0;
#endif

    /*
      Retrieves a bool at the current element.
     */
    virtual bool get_bool() const = 0;

    /*
      Returns true if and only if this iterator has another element
      past the current element.
     */
    virtual bool has_next() const = 0;

    /*
      Advances the iterator to the next element.
     */
    virtual void advance() = 0;

    /*
      Returns the number of elements this iterator processes. If this
      number is not known, std::numeric_limits<size_t>::max() is used
      instead.
     */
    virtual size_t size() const = 0;
  };
}
#endif
