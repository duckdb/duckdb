include_directories(../dbgen/include)
include_directories(include)

add_executable(sqlsmith
               duckdb.cc
               dump.cc
               expr.cc
               grammar.cc
               impedance.cc
               log.cc
               prod.cc
               random.cc
               relmodel.cc
               schema.cc
               sqlsmith.cc)
target_link_libraries(sqlsmith duckdb dbgen)
TARGET_LINK_LIBRARIES(sqlsmith pthread)


set_target_properties(
  sqlsmith
  PROPERTIES
    COMPILE_FLAGS
    "-Wno-potentially-evaluated-expression -Wno-delete-non-virtual-dtor")
