add_library_unity(test_mbedtls OBJECT test_mbedtls.cpp)
set(UNITTEST_OBJECT_FILES
    ${UNITTEST_OBJECT_FILES} $<TARGET_OBJECTS:test_mbedtls>
    PARENT_SCOPE)
