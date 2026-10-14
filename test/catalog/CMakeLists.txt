add_library_unity(test_catalog OBJECT test_catalog_version.cpp
                  test_system_catalog_pruning.cpp)

set(UNITTEST_OBJECT_FILES
    ${UNITTEST_OBJECT_FILES} $<TARGET_OBJECTS:test_catalog>
    PARENT_SCOPE)
