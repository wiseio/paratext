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
  These functions by Guillem Blanco are taken from WiseML.
 */

#ifndef PARATEXT_PARALLEL_HPP
#define PARATEXT_PARALLEL_HPP

#include <algorithm>

/*
 * Same as std::for_each but fun must have signature: void f(Iterator).
 */
template <typename Iterator, typename F>
inline F for_each_it(Iterator first, Iterator last, F &&fun) {
  for (; first != last; ++first)
    fun(first);
  return std::move(fun);
}

/*
 * Distributes the application of F in [first, last) among different threads.
 */
template <class Iterator, class F>
F parallel_for_each(Iterator first, Iterator last, size_t suggested_num_threads, F &&f) {
  using namespace std::placeholders;

  const std::size_t num_elements = std::distance(first, last);
  if (num_elements == 0) {
    return std::move(f);
  }
  const size_t num_threads =
      std::min(std::max(1UL, (unsigned long)suggested_num_threads), (unsigned long)num_elements);
  const std::size_t elements_thread = num_elements / num_threads;
  const std::size_t excess = num_elements % num_threads;

  /* Thread pool */
  std::vector<std::thread> thread_pool;
  thread_pool.reserve(num_threads);

  /* Spawn threads */
  Iterator it = first;
  for (std::size_t thread_id = 0; thread_id < num_threads; ++thread_id) {
    const std::size_t step = elements_thread + (thread_id < excess ? 1 : 0);
    thread_pool
        .emplace_back([ it, step, thread_id, f = std::forward<F>(f) ]() {
            for_each_it(it, it + step, std::bind(f, _1, thread_id));
        });
    it += step;
  }

  /* Join threads */
  for (auto &&thread : thread_pool) {
    thread.join();
  }
  return std::move(f);
}

#endif
