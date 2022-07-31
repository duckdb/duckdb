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

if(${CMAKE_VERSION} VERSION_GREATER_EQUAL "3.18.0")
find_package(Python3 COMPONENTS Interpreter Development.Module REQUIRED)
else()
find_package(Python3 COMPONENTS Interpreter Development REQUIRED)
endif()

# only Windows+MSVC seems to have trouble locating pybind11
if (MSVC)
  execute_process(COMMAND cmd.exe /c ${CMAKE_CURRENT_SOURCE_DIR}/pybind11Path.cmd "${Python3_EXECUTABLE}"
      WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
      OUTPUT_STRIP_TRAILING_WHITESPACE
      OUTPUT_VARIABLE EXTRA_PACKAGE_PATH)
  set(CMAKE_PREFIX_PATH ${CMAKE_PREFIX_PATH} ${EXTRA_PACKAGE_PATH})
endif()

find_package(pybind11 CONFIG REQUIRED)

pybind11_add_module(python MODULE EXCLUDE_FROM_ALL THIN_LTO)

target_link_libraries(python
  PRIVATE
    common
    hll
    kll
    cpc
    fi
    theta
    sampling
    req
    quantiles
    pybind11::module
)

set_target_properties(python PROPERTIES
  PREFIX ""
  OUTPUT_NAME datasketches
)

# ensure we make a .so on Mac rather than .dylib
if (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
  set_target_properties(python PROPERTIES SUFFIX ".so")
endif()

target_sources(python
  PRIVATE
    src/datasketches.cpp
    src/hll_wrapper.cpp
    src/kll_wrapper.cpp
    src/cpc_wrapper.cpp
    src/fi_wrapper.cpp
    src/theta_wrapper.cpp
    src/vo_wrapper.cpp
    src/req_wrapper.cpp
    src/quantiles_wrapper.cpp
    src/ks_wrapper.cpp
    src/vector_of_kll.cpp
)
