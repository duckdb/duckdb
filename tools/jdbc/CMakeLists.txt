find_package(Java 1.8)
find_package(JNI)

cmake_minimum_required(VERSION 3.11.0)

if(NOT JNI_FOUND OR NOT Java_FOUND)
  message(FATAL_ERROR "No compatible Java/JNI found")
endif()

include(UseJava)
project(DuckDBJDummy NONE)

if(WIN32)
  set(MVN_COMMAND cmd /c mvnw.bat)
else()
  set(MVN_COMMAND sh mvnw)
endif()

set(OUT_DIR ${CMAKE_CURRENT_SOURCE_DIR}/target/headers)

add_custom_command(
  OUTPUT ${OUT_DIR}/org_duckdb_DuckDBNative.h
  USES_TERMINAL
  COMMAND ${MVN_COMMAND} compile
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

add_custom_target(duckdb_jdbc DEPENDS ${OUT_DIR}/org_duckdb_DuckDBNative.h)

include_directories(${JNI_INCLUDE_DIRS} ${OUT_DIR}
                    ../../extension/parquet/include)

add_library(duckdb_java SHARED src/jni/duckdb_java.cpp)
add_dependencies(duckdb_java duckdb_jdbc)
target_compile_options(duckdb_java PRIVATE -fexceptions)
target_link_libraries(duckdb_java duckdb_static parquet_extension)
if(OS_NAME STREQUAL "linux")
  target_link_libraries(duckdb_java -static-libgcc -static-libstdc++
  )# static link to libstdc++ to target more linux distro
endif()
if(APPLE)
  set(OS_ARCH universal)
endif()
if(OVERRIDE_JDBC_OS_ARCH)
  set(OS_ARCH ${OVERRIDE_JDBC_OS_ARCH})
endif()
string(JOIN "_" LIB_SUFFIX ".so" ${OS_NAME} ${OS_ARCH})
set_target_properties(duckdb_java PROPERTIES SUFFIX ${LIB_SUFFIX})
set_target_properties(duckdb_java PROPERTIES PREFIX "lib")

add_custom_target(
  dummy_jdbc_target
  DEPENDS duckdb_java duckdb_jdbc
  USES_TERMINAL
  COMMAND
    ${CMAKE_COMMAND} -E copy
    $<TARGET_FILE_DIR:duckdb_java>/$<TARGET_FILE_NAME:duckdb_java>
    ${CMAKE_CURRENT_SOURCE_DIR}/src/main/resources
  COMMAND ${MVN_COMMAND} package -Dmaven.test.skip=true
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

add_custom_command(
  TARGET dummy_jdbc_target
  POST_BUILD
  COMMAND
    ${CMAKE_COMMAND} -E copy
    ${CMAKE_CURRENT_SOURCE_DIR}/target/duckdb_jdbc-1.0-SNAPSHOT.jar
    duckdb_jdbc.jar)

add_test(
  USES_TERMINAL
  COMMAND ${MVN_COMMAND} test
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

add_custom_target(jdbc ALL DEPENDS dummy_jdbc_target)
