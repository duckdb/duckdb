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

add_executable(sampling_test)

target_link_libraries(sampling_test sampling common_test)

set_target_properties(sampling_test PROPERTIES
  CXX_STANDARD 11
  CXX_STANDARD_REQUIRED YES
)

file(TO_CMAKE_PATH "${CMAKE_CURRENT_SOURCE_DIR}" SAMPLING_TEST_BINARY_PATH)
string(APPEND SAMPLING_TEST_BINARY_PATH "/")
target_compile_definitions(sampling_test
  PRIVATE
    TEST_BINARY_INPUT_PATH="${SAMPLING_TEST_BINARY_PATH}"
)

add_test(
  NAME sampling_test
  COMMAND sampling_test
)

target_sources(sampling_test
  PRIVATE
  var_opt_sketch_test.cpp
  var_opt_union_test.cpp
  var_opt_allocation_test.cpp
)
