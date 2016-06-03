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
      std::min(std::max(1UL, suggested_num_threads), num_elements);
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
