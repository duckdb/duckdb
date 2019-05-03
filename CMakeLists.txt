cmake_minimum_required(VERSION 2.8.7)
project(DuckDB)

find_package(Threads REQUIRED)


SET(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

SET(CMAKE_VERBOSE_MAKEFILE OFF)
SET(CMAKE_POSITION_INDEPENDENT_CODE ON)
SET(CMAKE_MACOSX_RPATH 1)

IF(NOT MSVC)
	SET(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g -O0 -DDEBUG -Wall")

	SET(CXX_EXTRA_DEBUG "-fsanitize=address -Wunused-variable -Wunused-const-variable")

	IF("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU" AND CMAKE_CXX_COMPILER_VERSION VERSION_GREATER 8.0)
		SET(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} ${CXX_EXTRA_DEBUG}")
	ELSEIF ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang" AND CMAKE_CXX_COMPILER_VERSION VERSION_GREATER 9.0)
		SET(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} ${CXX_EXTRA_DEBUG}")
	ELSE()
	    message(WARNING "Please use a recent compiler for debug builds")
	ENDIF()
ENDIF()


# todo use CHECK_CXX_COMPILER_FLAG(-fsanitize=address SUPPORTS_SANITIZER) etc.

SET(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -g -O3")

SET(CMAKE_C_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG}")
SET(CMAKE_C_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE}")

IF (NOT CMAKE_BUILD_TYPE)
    message(FATAL_ERROR "Choose the type of build by setting -DCMAKE_BUILD_TYPE=<type>, options are: Debug Release")
ENDIF()

include_directories(src/include)
include_directories(third_party/hyperloglog)
include_directories(third_party/re2)
include_directories(third_party/miniz)

add_subdirectory(src)
add_subdirectory(third_party)

IF (NOT LEAN)
	add_subdirectory(test)
	IF (NOT WIN32)
	  add_subdirectory(tools)
	  add_subdirectory(benchmark)
	ENDIF()
ENDIF()
