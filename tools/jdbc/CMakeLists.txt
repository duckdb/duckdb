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
add_jar(DuckDBJDummy ${JAVA_SRC_FILES} GENERATE_NATIVE_HEADERS duckdb-native)

add_library(duckdb_java SHARED src/jni/duckdb_java.cpp)
target_link_libraries(duckdb_java duckdb-native duckdb_static)
set_target_properties(duckdb_java PROPERTIES SUFFIX ".so")

add_custom_command(OUTPUT DuckDBJ.jar
                   COMMAND zip -O DuckDBJ.jar -rv DuckDBJDummy.jar libduckdb_java.so)

add_custom_target(full_out ALL
                  DEPENDS DuckDBJ.jar duckdb_java DuckDBJDummy)
