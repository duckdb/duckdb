/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#ifndef CONDITIONAL_FORWARD_HPP_
#define CONDITIONAL_FORWARD_HPP_

#include <type_traits>

namespace datasketches {

// Forward type T2 as rvalue reference if type T1 is rvalue reference

template<typename T1, typename T2>
using fwd_type = typename std::conditional<std::is_lvalue_reference<T1>::value,
    T2, typename std::remove_reference<T2>::type&&>::type;

template<typename T1, typename T2>
fwd_type<T1, T2> conditional_forward(T2&& value) {
  return std::forward<fwd_type<T1, T2>>(std::forward<T2>(value));
}

// Forward container as iterators

template<typename Container>
auto forward_begin(Container&& c) -> typename std::enable_if<
  std::is_lvalue_reference<Container>::value ||
  std::is_same<typename std::remove_reference<Container>::type::const_iterator, decltype(c.begin())>::value,
  decltype(c.begin())
>::type
{
  return c.begin();
}

template<typename Container>
auto forward_begin(Container&& c) -> typename std::enable_if<
  !std::is_lvalue_reference<Container>::value &&
  !std::is_same<typename std::remove_reference<Container>::type::const_iterator, decltype(c.begin())>::value,
  decltype(std::make_move_iterator(c.begin()))
>::type
{
  return std::make_move_iterator(c.begin());
}

template<typename Container>
auto forward_end(Container&& c) -> typename std::enable_if<
  std::is_lvalue_reference<Container>::value ||
  std::is_same<typename std::remove_reference<Container>::type::const_iterator, decltype(c.begin())>::value,
  decltype(c.end())
>::type
{
  return c.end();
}

template<typename Container>
auto forward_end(Container&& c) -> typename std::enable_if<
  !std::is_lvalue_reference<Container>::value &&
  !std::is_same<typename std::remove_reference<Container>::type::const_iterator, decltype(c.begin())>::value,
  decltype(std::make_move_iterator(c.end()))
>::type
{
  return std::make_move_iterator(c.end());
}

} /* namespace datasketches */

#endif
