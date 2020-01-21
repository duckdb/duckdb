add_definitions(-DDUCKDB)

if(NOT MSVC)
  set(
    CMAKE_CXX_FLAGS_DEBUG
    "${CMAKE_CXX_FLAGS_DEBUG} -Wextra -Wno-unused-parameter -Wno-redundant-move -Wnarrowing"
    )
endif()

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
      pg_query
      hyperloglog
      re2
      miniz)

  add_library(duckdb SHARED ${ALL_OBJECT_FILES} parser/query_node/recursive_cte_node.cpp include/duckdb/parser/query_node/recursive_cte_node.hpp include/duckdb/planner/query_node/bound_recursive_cte_node.hpp planner/binder/query_node/bind_recursive_cte_node.cpp include/duckdb/planner/tableref/bound_cteref.hpp include/duckdb/planner/operator/logical_recursive_cte.hpp include/duckdb/planner/operator/logical_cteref.hpp execution/operator/set/physical_recursive_cte.cpp include/duckdb/execution/operator/set/physical_recursive_cte.hpp execution/operator/scan/physical_cte_scan.cpp include/duckdb/execution/operator/scan/physical_cte_scan.hpp)
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
