add_library_unity(test_sakila OBJECT test_sakila.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sakila>
    PARENT_SCOPE)
