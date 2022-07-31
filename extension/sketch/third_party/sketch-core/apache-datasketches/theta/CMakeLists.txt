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

add_library(theta INTERFACE)

add_library(${PROJECT_NAME}::THETA ALIAS theta)

if (BUILD_TESTS)
  add_subdirectory(test)
endif()

target_include_directories(theta
  INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:$<INSTALL_PREFIX>/include>
)

target_link_libraries(theta INTERFACE common)
target_compile_features(theta INTERFACE cxx_std_11)

install(TARGETS theta
  EXPORT ${PROJECT_NAME}
)

install(FILES 
			include/theta_sketch.hpp
			include/theta_sketch_impl.hpp
			include/theta_union.hpp
			include/theta_union_impl.hpp
			include/theta_intersection.hpp
			include/theta_intersection_impl.hpp
			include/theta_a_not_b.hpp
			include/theta_a_not_b_impl.hpp
			include/theta_jaccard_similarity.hpp
			include/theta_comparators.hpp
			include/theta_constants.hpp
			include/theta_helpers.hpp
			include/theta_update_sketch_base.hpp
			include/theta_update_sketch_base_impl.hpp
			include/theta_union_base.hpp
			include/theta_union_base_impl.hpp
			include/theta_intersection_base.hpp
			include/theta_intersection_base_impl.hpp
			include/theta_set_difference_base.hpp
			include/theta_set_difference_base_impl.hpp
			include/theta_jaccard_similarity_base.hpp
			include/bounds_on_ratios_in_sampled_sets.hpp
			include/bounds_on_ratios_in_theta_sketched_sets.hpp
			include/compact_theta_sketch_parser.hpp
			include/compact_theta_sketch_parser_impl.hpp
  DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/DataSketches")
