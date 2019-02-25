# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.


set(ARROW_SRCS
    array.cc
    builder.cc
    array/builder_adaptive.cc
    array/builder_base.cc
    array/builder_binary.cc
    array/builder_decimal.cc
    array/builder_dict.cc
    array/builder_nested.cc
    array/builder_primitive.cc
    array/builder_union.cc
    buffer.cc
    compare.cc
    memory_pool.cc
    pretty_print.cc
    record_batch.cc
    scalar.cc
    sparse_tensor.cc
    status.cc
    table.cc
    table_builder.cc
  #  tensor.cc
    type.cc
    visitor.cc
#    csv/converter.cc
#    csv/chunker.cc
#    csv/column-builder.cc
#    csv/options.cc
#    csv/parser.cc
#    csv/reader.cc
#    json/options.cc
#    json/chunker.cc
#    json/parser.cc
#    json/reader.cc
#    io/buffered.cc
#    io/compressed.cc
#    io/file.cc
#    io/interfaces.cc
#    io/memory.cc
#    io/readahead.cc
#    testing/util.cc
    util/basic_decimal.cc
    util/bit-util.cc
    util/compression.cc
    util/cpu-info.cc
    util/decimal.cc
    util/int-util.cc
    util/io-util.cc
    util/logging.cc
    util/key_value_metadata.cc
    util/memory.cc
    util/task-group.cc
    util/thread-pool.cc
    util/trie.cc
    util/utf8.cc
    vendored/datetime/tz.cpp)

if("${COMPILER_FAMILY}" STREQUAL "clang")
  set_property(SOURCE util/io-util.cc
               APPEND_STRING
               PROPERTY COMPILE_FLAGS " -Wno-unused-macros ")
endif()

include_directories(..)
#include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src/postgres/include)


add_library(arrow STATIC ${ARROW_SRCS})




