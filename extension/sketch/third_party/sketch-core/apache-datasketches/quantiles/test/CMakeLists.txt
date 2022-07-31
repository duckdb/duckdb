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

add_executable(quantiles_test)

target_link_libraries(quantiles_test quantiles common common_test)

set_target_properties(quantiles_test PROPERTIES
  CXX_STANDARD 11
  CXX_STANDARD_REQUIRED YES
)

file(TO_CMAKE_PATH "${CMAKE_CURRENT_SOURCE_DIR}" QUANTILES_TEST_BINARY_PATH)
string(APPEND QUANTILES_TEST_BINARY_PATH "/")
target_compile_definitions(quantiles_test
  PRIVATE
    TEST_BINARY_INPUT_PATH="${QUANTILES_TEST_BINARY_PATH}"
)

add_test(
  NAME quantiles_test
  COMMAND quantiles_test
)

target_sources(quantiles_test
  PRIVATE
    quantiles_sketch_test.cpp
    quantiles_compatibility_test.cpp
    kolmogorov_smirnov_test.cpp
)
