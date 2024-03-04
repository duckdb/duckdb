add_library_unity(odbc_connect OBJECT connect.cpp connection.cpp
                  driver_connect.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:odbc_connect>
    PARENT_SCOPE)
