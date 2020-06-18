add_definitions(-DDUCKDB)

if(NOT MSVC)
  set(
    CMAKE_CXX_FLAGS_DEBUG
    "${CMAKE_CXX_FLAGS_DEBUG} -Wextra -Wno-unused-parameter -Wno-redundant-move -Wnarrowing"
    )
endif()

# clang-tidy config from presto-cpp
# set(CMAKE_CXX_CLANG_TIDY clang-tidy --checks=*,-abseil-*,-android-*,-cert-err58-cpp,-clang-analyzer-osx-*,-cppcoreguidelines-avoid-c-arrays,-cppcoreguidelines-avoid-magic-numbers,-cppcoreguidelines-pro-bounds-array-to-pointer-decay,-cppcoreguidelines-pro-bounds-pointer-arithmetic,-cppcoreguidelines-pro-type-reinterpret-cast,-cppcoreguidelines-pro-type-vararg,-fuchsia-*,-google-*,-hicpp-avoid-c-arrays,-hicpp-deprecated-headers,-hicpp-no-array-decay,-hicpp-use-equals-default,-hicpp-vararg,-llvmlibc-*,-llvm-header-guard,-llvm-include-order,-mpi-*,-misc-non-private-member-variables-in-classes,-misc-no-recursion,-misc-unused-parameters,-modernize-avoid-c-arrays,-modernize-deprecated-headers,-modernize-use-nodiscard,-modernize-use-trailing-return-type,-objc-*,-openmp-*,-readability-avoid-const-params-in-decls,-readability-convert-member-functions-to-static,-readability-magic-numbers,-zircon-*)



if(AMALGAMATION_BUILD)

  if(WIN32)
    add_definitions(/bigobj)
  endif()

  add_library(duckdb SHARED "${CMAKE_SOURCE_DIR}/src/amalgamation/duckdb.cpp")
  add_library(duckdb_static STATIC
              "${CMAKE_SOURCE_DIR}/src/amalgamation/duckdb.cpp")

  install(FILES "${CMAKE_SOURCE_DIR}/src/amalgamation/duckdb.hpp"
          DESTINATION "${INSTALL_INCLUDE_DIR}")

else()

  add_subdirectory(optimizer)
  add_subdirectory(planner)
  add_subdirectory(parser)
  add_subdirectory(function)
  add_subdirectory(catalog)
  add_subdirectory(common)
  add_subdirectory(execution)
  add_subdirectory(main)
  add_subdirectory(storage)
  add_subdirectory(transaction)

  set(DUCKDB_LINK_LIBS
      fmt
      pg_query
      hyperloglog
      re2
      miniz
      utf8proc
      Threads::Threads)

  add_library(duckdb SHARED ${ALL_OBJECT_FILES})
  target_link_libraries(duckdb ${DUCKDB_LINK_LIBS})

  add_library(duckdb_static STATIC ${ALL_OBJECT_FILES})
  target_link_libraries(duckdb_static ${DUCKDB_LINK_LIBS})

  target_include_directories(
    duckdb
    PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
           $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)

  target_include_directories(
    duckdb_static
    PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
           $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)

  install(DIRECTORY "${CMAKE_SOURCE_DIR}/src/include/duckdb"
          DESTINATION "${INSTALL_INCLUDE_DIR}"
          FILES_MATCHING
          PATTERN "*.hpp")
  install(FILES "${CMAKE_SOURCE_DIR}/src/include/duckdb.hpp"
                "${CMAKE_SOURCE_DIR}/src/include/duckdb.h"
          DESTINATION "${INSTALL_INCLUDE_DIR}")

endif()

install(TARGETS duckdb duckdb_static
        EXPORT "${DUCKDB_EXPORT_SET}"
        LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
        ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
