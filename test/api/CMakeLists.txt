
IF (NOT WIN32)
	add_library_unity(test_api OBJECT test_api.cpp test_results.cpp test_read_only.cpp test_query_profiler.cpp)
ELSE()
	add_library_unity(test_api OBJECT test_api.cpp test_results.cpp test_query_profiler.cpp)
ENDIF()

set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_api> PARENT_SCOPE)
