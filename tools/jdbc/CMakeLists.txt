find_package(Java 1.8)
find_package(JNI)

cmake_minimum_required(VERSION 3.11.0)


if(NOT JNI_FOUND OR NOT Java_FOUND)
  message(FATAL_ERROR No
                      compatible
                      Java/JNI
                      found)
endif()

include(UseJava)
project(DuckDBJDummy NONE)

file(GLOB JAVA_SRC_FILES src/main/java/nl/cwi/da/duckdb/*.java)
file(GLOB JAVA_TEST_FILES src/test/java/nl/cwi/da/duckdb/test/*.java)

add_jar(duckdb_jdbc
        ${JAVA_SRC_FILES}
        ${JAVA_TEST_FILES}
        GENERATE_NATIVE_HEADERS
        duckdb-native)

add_library(duckdb_java SHARED src/jni/duckdb_java.cpp)
target_compile_options(duckdb_java PRIVATE -fexceptions)
target_link_libraries(duckdb_java duckdb-native duckdb_static)
set_target_properties(duckdb_java PROPERTIES SUFFIX ".so")
set_target_properties(duckdb_java PROPERTIES PREFIX "lib")

add_custom_command(OUTPUT dummy_jdbc_target
                   DEPENDS duckdb_java duckdb_jdbc
                   COMMAND ${Java_JAR_EXECUTABLE}
                           uf
                           duckdb_jdbc.jar
                           -C
                           $<TARGET_FILE_DIR:duckdb_java>
                           $<TARGET_FILE_NAME:duckdb_java>)

add_custom_target(jdbc ALL DEPENDS dummy_jdbc_target)
