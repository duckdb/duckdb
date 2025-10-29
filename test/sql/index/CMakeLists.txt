add_library_unity(test_index OBJECT test_art_index.cpp test_art_keys.cpp
                  art/pretty_printer/test_art_pretty_printer.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_index>
    PARENT_SCOPE)
