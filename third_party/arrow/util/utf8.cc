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

#include <mutex>

#include "arrow/util/logging.h"
#include "arrow/util/utf8.h"

namespace arrow {
namespace util {
namespace internal {

// Copyright (c) 2008-2010 Bjoern Hoehrmann <bjoern@hoehrmann.de>
// See http://bjoern.hoehrmann.de/utf-8/decoder/dfa/ for details.

// clang-format off
const uint8_t utf8_small_table[] = { // NOLINT
  // The first part of the table maps bytes to character classes that
  // to reduce the size of the transition table and create bitmasks.
   0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,  0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,  // NOLINT
   0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,  0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,  // NOLINT
   0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,  0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,  // NOLINT
   0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,  0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,  // NOLINT
   1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,  9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,  // NOLINT
   7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,  7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,  // NOLINT
   8,8,2,2,2,2,2,2,2,2,2,2,2,2,2,2,  2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,  // NOLINT
  10,3,3,3,3,3,3,3,3,3,3,3,3,4,3,3, 11,6,6,6,5,8,8,8,8,8,8,8,8,8,8,8,  // NOLINT

  // The second part is a transition table that maps a combination
  // of a state of the automaton and a character class to a state.
  // Character classes are between 0 and 11, states are multiples of 12.
   0,12,24,36,60,96,84,12,12,12,48,72, 12,12,12,12,12,12,12,12,12,12,12,12,  // NOLINT
  12, 0,12,12,12,12,12, 0,12, 0,12,12, 12,24,12,12,12,12,12,24,12,24,12,12,  // NOLINT
  12,12,12,12,12,12,12,24,12,12,12,12, 12,24,12,12,12,12,12,12,12,24,12,12,  // NOLINT
  12,12,12,12,12,12,12,36,12,36,12,12, 12,36,12,12,12,12,12,36,12,36,12,12,  // NOLINT
  12,36,12,12,12,12,12,12,12,12,12,12,  // NOLINT
};
// clang-format on

uint16_t utf8_large_table[9 * 256] = {0xffff};

static void InitializeLargeTable() {
  for (uint32_t state = 0; state < 9; ++state) {
    for (uint32_t byte = 0; byte < 256; ++byte) {
      uint32_t byte_class = utf8_small_table[byte];
      uint8_t next_state = utf8_small_table[256 + state * 12 + byte_class] / 12;
      DCHECK_LT(next_state, 9);
      utf8_large_table[state * 256 + byte] = static_cast<uint16_t>(next_state * 256);
    }
  }
}

#ifndef NDEBUG
ARROW_EXPORT void CheckUTF8Initialized() {
  DCHECK_EQ(utf8_large_table[0], 0)
      << "InitializeUTF8() must be called before calling UTF8 routines";
}
#endif

}  // namespace internal

static std::once_flag utf8_initialized;

void InitializeUTF8() {
  std::call_once(utf8_initialized, internal::InitializeLargeTable);
}

}  // namespace util
}  // namespace arrow
