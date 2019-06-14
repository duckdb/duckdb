add_library_unity(test_common OBJECT test_cast.cpp test_ops.cpp test_checksum.cpp test_date.cpp test_file_system.cpp test_gzip_stream.cpp test_hyperloglog.cpp test_sort.cpp test_utf.cpp) # test_serializer.cpp
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_common> PARENT_SCOPE)
