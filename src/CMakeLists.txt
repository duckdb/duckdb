add_definitions(-DDUCKDB)

if(${DISABLE_THREADS})
  add_definitions(-DDUCKDB_NO_THREADS)
endif()

if(NOT MSVC)
  set(CMAKE_CXX_FLAGS_DEBUG
      "${CMAKE_CXX_FLAGS_DEBUG} -Wextra -Wno-unused-parameter -Wno-redundant-move"
  )
endif()

# clang-tidy config from presto-cpp set(CMAKE_CXX_CLANG_TIDY clang-tidy
# --checks=*,-abseil-*,-android-*,-cert-err58-cpp,-clang-analyzer-
# osx-*,-cppcoreguidelines-avoid-c-arrays,-cppcoreguidelines-avoid-magic-
# numbers,-cppcoreguidelines-pro-bounds-array-to-pointer-
# decay,-cppcoreguidelines-pro-bounds-pointer-arithmetic,-cppcoreguidelines-pro-
# type-reinterpret-cast,-cppcoreguidelines-pro-type-
# vararg,-fuchsia-*,-google-*,-hicpp-avoid-c-arrays,-hicpp-deprecated-
# headers,-hicpp-no-array-decay,-hicpp-use-equals-default,-hicpp-
# vararg,-llvmlibc-*,-llvm-header-guard,-llvm-include-order,-mpi-*,-misc-non-
# private-member-variables-in-classes,-misc-no-recursion,-misc-unused-
# parameters,-modernize-avoid-c-arrays,-modernize-deprecated-headers,-modernize-
# use-nodiscard,-modernize-use-trailing-return-
# type,-objc-*,-openmp-*,-readability-avoid-const-params-in-decls,-readability-
# convert-member-functions-to-static,-readability-magic-numbers,-zircon-*)

if(AMALGAMATION_BUILD)

  if(WIN32)
    add_definitions(/bigobj)
  endif()

  add_library(duckdb SHARED "${PROJECT_SOURCE_DIR}/src/amalgamation/duckdb.cpp")
  target_link_libraries(duckdb ${CMAKE_DL_LIBS})
  link_threads(duckdb)

  add_library(duckdb_static STATIC
              "${PROJECT_SOURCE_DIR}/src/amalgamation/duckdb.cpp")
  target_link_libraries(duckdb_static ${CMAKE_DL_LIBS})
  link_threads(duckdb_static)

  install(FILES "${PROJECT_SOURCE_DIR}/src/amalgamation/duckdb.hpp"
                "${PROJECT_SOURCE_DIR}/src/include/duckdb.h"
          DESTINATION "${INSTALL_INCLUDE_DIR}")
  install(FILES "${PROJECT_SOURCE_DIR}/src/include/duckdb/common/winapi.hpp"
          DESTINATION "${INSTALL_INCLUDE_DIR}/duckdb/common")

else()

  add_definitions(-DDUCKDB_MAIN_LIBRARY)

  add_subdirectory(optimizer)
  add_subdirectory(planner)
  add_subdirectory(parser)
  add_subdirectory(function)
  add_subdirectory(catalog)
  add_subdirectory(common)
  add_subdirectory(execution)
  add_subdirectory(main)
  add_subdirectory(parallel)
  add_subdirectory(storage)
  add_subdirectory(transaction)

  set(DUCKDB_LINK_LIBS
      ${CMAKE_DL_LIBS}
      fmt
      pg_query
      duckdb_re2
      miniz
      utf8proc
      hyperloglog
      fastpforlib)

  add_library(duckdb SHARED ${ALL_OBJECT_FILES})
  target_link_libraries(duckdb ${DUCKDB_LINK_LIBS})
  link_threads(duckdb)

  add_library(duckdb_static STATIC ${ALL_OBJECT_FILES})
  target_link_libraries(duckdb_static ${DUCKDB_LINK_LIBS})
  link_threads(duckdb_static)

  target_include_directories(
    duckdb PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
                  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)

  target_include_directories(
    duckdb_static PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
                         $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)

  install(
    DIRECTORY "${PROJECT_SOURCE_DIR}/src/include/duckdb"
    DESTINATION "${INSTALL_INCLUDE_DIR}"
    FILES_MATCHING
    PATTERN "*.hpp")
  install(FILES "${PROJECT_SOURCE_DIR}/src/include/duckdb.hpp"
                "${PROJECT_SOURCE_DIR}/src/include/duckdb.h"
          DESTINATION "${INSTALL_INCLUDE_DIR}")

endif()

if(BUILD_PYTHON
   OR BUILD_R
   OR CONFIGURE_R)
  if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(ALL_COMPILE_FLAGS "${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_DEBUG}")
  elseif(CMAKE_BUILD_TYPE STREQUAL "Release")
    set(ALL_COMPILE_FLAGS "${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_RELEASE}")
  elseif(CMAKE_BUILD_TYPE STREQUAL "RelWithDebInfo")
    set(ALL_COMPILE_FLAGS
        "${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_RELWITHDEBINFO}")
  else()
    set(ALL_COMPILE_FLAGS "${CMAKE_CXX_FLAGS}")
  endif()

  get_target_property(duckdb_libs duckdb LINK_LIBRARIES)

  if(BUILD_PYTHON)
    if(USER_SPACE)
      add_custom_target(
        duckdb_python ALL
        COMMAND python3 setup.py install --user --binary-dir=${CMAKE_BINARY_DIR}
                --compile-flags=${ALL_COMPILE_FLAGS} --libs="${duckdb_libs}"
        DEPENDS duckdb
        WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}/tools/pythonpkg
        COMMENT Build
        Python package)
    else()
      add_custom_target(
        duckdb_python ALL
        COMMAND python3 setup.py install --binary-dir=${CMAKE_BINARY_DIR}
                --compile-flags=${ALL_COMPILE_FLAGS} --libs="${duckdb_libs}"
        DEPENDS duckdb
        WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}/tools/pythonpkg
        COMMENT Build
        Python package)
    endif()
    add_extension_dependencies(duckdb_python)
  endif()
  if(CONFIGURE_R)
    add_custom_target(
      duckdb_configure_r ALL
      COMMAND
        DUCKDB_R_BINDIR=${CMAKE_BINARY_DIR}
        DUCKDB_R_CFLAGS=\"${ALL_COMPILE_FLAGS}\"
        DUCKDB_R_LIBS=\"${duckdb_libs}\" python rconfigure.py
      DEPENDS duckdb
      WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}/tools/rpkg
      COMMENT Configure
      R package)
    add_extension_dependencies(duckdb_configure_r)
  endif()
  if(BUILD_R)
    add_custom_target(
      duckdb_r ALL
      COMMAND
        DUCKDB_R_BINDIR=${CMAKE_BINARY_DIR}
        DUCKDB_R_CFLAGS=\"${ALL_COMPILE_FLAGS}\"
        DUCKDB_R_LIBS=\"${duckdb_libs}\" DUCKDB_R_DEBUG= R CMD INSTALL .
      DEPENDS duckdb
      WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}/tools/rpkg
      COMMENT Build
      R package)
    add_extension_dependencies(duckdb_r)
  endif()
endif()

install(
  TARGETS duckdb duckdb_static
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
