find_package(Java 1.7)
find_package(JNI)

if(NOT JNI_FOUND OR NOT Java_FOUND)
  message(FATAL_ERROR No
                      compatible
                      Java/JNI
                      found)
endif()

include_directories(src/jni)
include_directories(${JNI_INCLUDE_DIRS})

add_library(duckdb_java SHARED src/jni/duckdb_java.cpp)
target_link_libraries(duckdb_java duckdb_static)

include(UseJava)
project(DuckDBJ NONE)
file(GLOB JAVA_SRC_FILES src/main/java/nl/cwi/da/duckdb/*.java)
add_jar(DuckDBJ ${JAVA_SRC_FILES})
get_target_property(_jarFile DuckDBJ JAR_FILE)
# add_test(NAME TestHelloWorld COMMAND ${Java_JAVA_EXECUTABLE} -cp ${_jarFile}
# HelloWorld)
