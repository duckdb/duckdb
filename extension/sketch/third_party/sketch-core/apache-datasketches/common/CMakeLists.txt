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

add_library(common INTERFACE)

if (BUILD_TESTS)
  add_subdirectory(test)
endif()

target_include_directories(common
  INTERFACE
    $<INSTALL_INTERFACE:$<INSTALL_PREFIX>/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
)

target_compile_features(common INTERFACE cxx_std_11)

install(TARGETS common EXPORT ${PROJECT_NAME})

install(FILES 
			include/common_defs.hpp
			include/memory_operations.hpp
			include/MurmurHash3.h
			include/serde.hpp
			include/count_zeros.hpp
			include/inv_pow2_table.hpp
			include/binomial_bounds.hpp
			include/conditional_back_inserter.hpp
			include/conditional_forward.hpp
			include/ceiling_power_of_2.hpp
			include/bounds_binomial_proportions.hpp
			include/quantile_sketch_sorted_view.hpp
			include/quantile_sketch_sorted_view_impl.hpp
			include/kolmogorov_smirnov.hpp
			include/kolmogorov_smirnov_impl.hpp
  DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/DataSketches")
