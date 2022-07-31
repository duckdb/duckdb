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

add_executable(req_test)

target_link_libraries(req_test req common_test)

set_target_properties(req_test PROPERTIES
  CXX_STANDARD 11
  CXX_STANDARD_REQUIRED YES
)

file(TO_CMAKE_PATH "${CMAKE_CURRENT_SOURCE_DIR}" REQ_TEST_BINARY_PATH)
string(APPEND REQ_TEST_BINARY_PATH "/")
target_compile_definitions(req_test
  PRIVATE
    TEST_BINARY_INPUT_PATH="${REQ_TEST_BINARY_PATH}"
)

add_test(
  NAME req_test
  COMMAND req_test
)

target_sources(req_test
  PRIVATE
    req_sketch_test.cpp
    req_sketch_custom_type_test.cpp
)
