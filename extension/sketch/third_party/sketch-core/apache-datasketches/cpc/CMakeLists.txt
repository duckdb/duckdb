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

add_library(cpc INTERFACE)

add_library(${PROJECT_NAME}::CPC ALIAS cpc)

if (BUILD_TESTS)
  add_subdirectory(test)
endif()

target_include_directories(cpc
  INTERFACE
    $<INSTALL_INTERFACE:$<INSTALL_PREFIX>/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
)

target_link_libraries(cpc INTERFACE common)
target_compile_features(cpc INTERFACE cxx_std_11)

install(TARGETS cpc
  EXPORT ${PROJECT_NAME}
)

install(FILES 
			include/compression_data.hpp
			include/cpc_common.hpp
			include/cpc_compressor.hpp
			include/cpc_compressor_impl.hpp
			include/cpc_confidence.hpp
			include/cpc_sketch.hpp
			include/cpc_sketch_impl.hpp
			include/cpc_union.hpp
			include/cpc_union_impl.hpp
			include/cpc_util.hpp
			include/icon_estimator.hpp
			include/kxp_byte_lookup.hpp
			include/u32_table.hpp
			include/u32_table_impl.hpp
  DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/DataSketches")
