find_package(Java 1.8)
find_package(JNI)

cmake_minimum_required(VERSION 3.11.0)

if(NOT JNI_FOUND OR NOT Java_FOUND)
  message(FATAL_ERROR "No compatible Java/JNI found")
endif()

include(UseJava)
project(DuckDBJDummy NONE)

file(GLOB JAVA_SRC_FILES src/main/java/org/duckdb/*.java)
file(GLOB JAVA_TEST_FILES src/test/java/org/duckdb/test/*.java)

set(CMAKE_JAVA_COMPILE_FLAGS -source 1.8 -target 1.8 -encoding utf-8)

add_jar(duckdb_jdbc ${JAVA_SRC_FILES} ${JAVA_TEST_FILES}
        META-INF/services/java.sql.Driver GENERATE_NATIVE_HEADERS duckdb-native)

include_directories(../../extension/parquet/include)

add_library(duckdb_java SHARED src/jni/duckdb_java.cpp src/jni/functions.cpp)
target_compile_options(duckdb_java PRIVATE -fexceptions)
target_link_libraries(duckdb_java duckdb-native duckdb_static parquet_extension)

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
  OUTPUT dummy_jdbc_target
  DEPENDS duckdb_java duckdb_jdbc
  COMMAND ${Java_JAR_EXECUTABLE} uf duckdb_jdbc.jar -C
          $<TARGET_FILE_DIR:duckdb_java> $<TARGET_FILE_NAME:duckdb_java>)

add_custom_target(jdbc ALL DEPENDS dummy_jdbc_target)
