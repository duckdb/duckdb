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

#ifndef CLASS_TEST_TYPE_HPP_
#define CLASS_TEST_TYPE_HPP_

#include <cstring>
#include <iostream>
#include "memory_operations.hpp"

namespace datasketches {

template<typename A>
class test_type_alloc {
  static const bool DEBUG = false;
public:
  // no default constructor should be required
  test_type_alloc(int value): value_ptr(A().allocate(1)) {
    if (DEBUG) std::cerr << "test_type constructor" << std::endl;
    *value_ptr = value;
  }
  ~test_type_alloc() {
    if (DEBUG) std::cerr << "test_type destructor" << std::endl;
    if (value_ptr != nullptr) A().deallocate(value_ptr, 1);
  }
  test_type_alloc(const test_type_alloc& other): value_ptr(A().allocate(1)) {
    if (DEBUG) std::cerr << "test_type copy constructor" << std::endl;
    *value_ptr = *other.value_ptr;
  }
  // noexcept is important here so that, for instance, std::vector could move this type
  test_type_alloc(test_type_alloc&& other) noexcept : value_ptr(nullptr) {
    if (DEBUG) std::cerr << "test_type move constructor" << std::endl;
    if (DEBUG && other.value_ptr == nullptr) std::cerr << "moving null" << std::endl;
    std::swap(value_ptr, other.value_ptr);
  }
  test_type_alloc& operator=(const test_type_alloc& other) {
    if (DEBUG) std::cerr << "test_type copy assignment" << std::endl;
    if (DEBUG && value_ptr == nullptr) std::cerr << "nullptr" << std::endl;
    *value_ptr = *other.value_ptr;
    return *this;
  }
  test_type_alloc& operator=(test_type_alloc&& other) {
    if (DEBUG) std::cerr << "test_type move assignment" << std::endl;
    if (DEBUG && other.value_ptr == nullptr) std::cerr << "moving null" << std::endl;
    std::swap(value_ptr, other.value_ptr);
    return *this;
  }
  int get_value() const {
    if (value_ptr == nullptr) std::cerr << "null" << std::endl;
    return *value_ptr;
  }
private:
  int* value_ptr;
};

using test_type = test_type_alloc<std::allocator<int>>;

struct test_type_hash {
  std::size_t operator()(const test_type& a) const {
    return std::hash<int>()(a.get_value());
  }
};

struct test_type_equal {
  bool operator()(const test_type& a1, const test_type& a2) const {
    return a1.get_value() == a2.get_value();
  }
};

struct test_type_less {
  bool operator()(const test_type& a1, const test_type& a2) const {
    return a1.get_value() < a2.get_value();
  }
};

struct test_type_serde {
  void serialize(std::ostream& os, const test_type* items, unsigned num) const {
    for (unsigned i = 0; i < num; i++) {
      const int value = items[i].get_value();
      os.write((char*)&value, sizeof(value));
    }
  }
  void deserialize(std::istream& is, test_type* items, unsigned num) const {
    for (unsigned i = 0; i < num; i++) {
      int value;
      is.read((char*)&value, sizeof(value));
      new (&items[i]) test_type(value);
    }
  }
  size_t size_of_item(const test_type&) const {
    return sizeof(int);
  }
  size_t serialize(void* ptr, size_t capacity, const test_type* items, unsigned num) const {
    const size_t bytes_written = sizeof(int) * num;
    check_memory_size(bytes_written, capacity);
    for (unsigned i = 0; i < num; ++i) {
      const int value = items[i].get_value();
      memcpy(ptr, &value, sizeof(int));
      ptr = static_cast<char*>(ptr) + sizeof(int);
    }
    return bytes_written;
  }
  size_t deserialize(const void* ptr, size_t capacity, test_type* items, unsigned num) const {
    const size_t bytes_read = sizeof(int) * num;
    check_memory_size(bytes_read, capacity);
    for (unsigned i = 0; i < num; ++i) {
      int value;
      memcpy(&value, ptr, sizeof(int));
      new (&items[i]) test_type(value);
      ptr = static_cast<const char*>(ptr) + sizeof(int);
    }
    return bytes_read;
  }
};

std::ostream& operator<<(std::ostream& os, const test_type& a) {
  os << a.get_value();
  return os;
}

} /* namespace datasketches */

#endif
