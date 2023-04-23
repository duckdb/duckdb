find_package(Java 1.8)
find_package(JNI)

cmake_minimum_required(VERSION 3.11.0)

if(NOT JNI_FOUND OR NOT Java_FOUND)
  message(FATAL_ERROR "No compatible Java/JNI found")
endif()

project(DuckDBJDummy NONE)

if(JNI_FOUND)
  message(STATUS "JNI_INCLUDE_DIRS=${JNI_INCLUDE_DIRS}")
  message(STATUS "JNI_LIBRARIES=${JNI_LIBRARIES}")
endif()

include_directories(../../extension/parquet/include)

if(OS_NAME STREQUAL "windows")
  set(GRADLEW ./gradlew.bat)
else()
  set(GRADLEW ./gradlew)
endif()

add_custom_command(
  OUTPUT duckdb_jni_header
  COMMAND ${GRADLEW} compileJava
  WORKING_DIRECTORY ${PROJECT_SOURCE_DIR})

add_library(duckdb_java SHARED src/jni/duckdb_java.cpp duckdb_jni_header)
target_include_directories(duckdb_java PUBLIC ./build/target/headers
                                              ${JNI_INCLUDE_DIRS})
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

add_custom_command(
  OUTPUT duckdb_jdbc
  DEPENDS duckdb_java
  COMMAND ${GRADLEW} publishToMavenLocal
  WORKING_DIRECTORY ${PROJECT_SOURCE_DIR})

add_custom_command(
  OUTPUT duckdb_jdbc_test
  DEPENDS duckdb_java
  COMMAND ${GRADLEW} test
  WORKING_DIRECTORY ${PROJECT_SOURCE_DIR})

add_custom_command(
  OUTPUT dummy_jdbc_target
  DEPENDS duckdb_jdbc
  COMMAND cp ${PROJECT_SOURCE_DIR}/build/libs/jdbc-*.jar
          $<TARGET_FILE_DIR:duckdb_java>)

add_custom_target(jdbc ALL DEPENDS dummy_jdbc_target)
