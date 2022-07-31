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

add_library(hll INTERFACE)

add_library(${PROJECT_NAME}::HLL ALIAS hll)

if (BUILD_TESTS)
  add_subdirectory(test)
endif()

target_include_directories(hll
  INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:$<INSTALL_PREFIX>/include>
)

target_link_libraries(hll INTERFACE common)
target_compile_features(hll INTERFACE cxx_std_11)

install(TARGETS hll
  EXPORT ${PROJECT_NAME}
)

install(FILES 
			include/hll.hpp
			include/AuxHashMap.hpp
			include/CompositeInterpolationXTable.hpp
			include/hll.private.hpp
			include/HllSketchImplFactory.hpp
			include/CouponHashSet.hpp
			include/CouponList.hpp
			include/CubicInterpolation.hpp
			include/HarmonicNumbers.hpp
			include/Hll4Array.hpp
			include/Hll6Array.hpp
			include/Hll8Array.hpp
			include/HllArray.hpp
			include/HllSketchImpl.hpp
			include/HllUtil.hpp
			include/coupon_iterator.hpp
			include/RelativeErrorTables.hpp
			include/AuxHashMap-internal.hpp
			include/CompositeInterpolationXTable-internal.hpp
			include/CouponHashSet-internal.hpp
			include/CouponList-internal.hpp
			include/CubicInterpolation-internal.hpp
			include/HarmonicNumbers-internal.hpp
			include/Hll4Array-internal.hpp
			include/Hll6Array-internal.hpp
			include/Hll8Array-internal.hpp
			include/HllArray-internal.hpp
			include/HllSketch-internal.hpp
			include/HllSketchImpl-internal.hpp
			include/HllUnion-internal.hpp
			include/coupon_iterator-internal.hpp
			include/RelativeErrorTables-internal.hpp
  DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/DataSketches")
