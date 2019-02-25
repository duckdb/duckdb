// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <type_traits>
#include <typeinfo>

#include <gtest/gtest.h>

#include "arrow/util/checked_cast.h"

namespace arrow {
namespace internal {

class Foo {
 public:
  virtual ~Foo() = default;
};

class Bar {};
class FooSub : public Foo {};
template <typename T>
class Baz : public Foo {};

TEST(CheckedCast, TestInvalidSubclassCast) {
  static_assert(std::is_polymorphic<Foo>::value, "Foo is not polymorphic");

  Foo foo;
  FooSub foosub;
  const Foo& foosubref = foosub;
  Baz<double> baz;
  const Foo& bazref = baz;

#ifndef NDEBUG  // debug mode
  // illegal pointer cast
  ASSERT_EQ(nullptr, checked_cast<Bar*>(&foo));

  // illegal reference cast
  ASSERT_THROW(checked_cast<const Bar&>(foosubref), std::bad_cast);

  // legal reference casts
  ASSERT_NO_THROW(checked_cast<const FooSub&>(foosubref));
  ASSERT_NO_THROW(checked_cast<const Baz<double>&>(bazref));
#else  // release mode
  // failure modes for the invalid casts occur at compile time

  // legal pointer cast
  ASSERT_NE(nullptr, checked_cast<const FooSub*>(&foosubref));

  // legal reference casts: this is static_cast in a release build, so ASSERT_NO_THROW
  // doesn't make a whole lot of sense here.
  auto& x = checked_cast<const FooSub&>(foosubref);
  ASSERT_EQ(&foosubref, &x);

  auto& y = checked_cast<const Baz<double>&>(bazref);
  ASSERT_EQ(&bazref, &y);
#endif
}

}  // namespace internal
}  // namespace arrow
