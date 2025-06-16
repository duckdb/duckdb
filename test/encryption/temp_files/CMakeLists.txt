add_library_unity(test_temp_file_encryption
        OBJECT
        basic_temp_file_encryption.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_temp_file_encryption>
        PARENT_SCOPE)