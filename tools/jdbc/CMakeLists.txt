find_package(Java 1.8)
find_package(JNI)

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

add_jar(DuckDBJDummy ${JAVA_SRC_FILES} ${JAVA_TEST_FILES} GENERATE_NATIVE_HEADERS duckdb-native)

add_library(duckdb_java SHARED src/jni/duckdb_java.cpp)
target_compile_options(duckdb_java PRIVATE -fexceptions)
target_link_libraries(duckdb_java duckdb-native duckdb_static)
set_target_properties(duckdb_java PROPERTIES SUFFIX ".so")
set_target_properties(duckdb_java PROPERTIES PREFIX "lib")

add_custom_command(OUTPUT DuckDBJ.jar DEPENDS duckdb_java DuckDBJDummy
                   COMMAND zip -j -O DuckDBJ.jar -rv DuckDBJDummy.jar $<TARGET_FILE:duckdb_java>)

add_custom_target(jdbc ALL DEPENDS DuckDBJ.jar)
