add_library_unity(duckdb_operator_helper
                  OBJECT
                  physical_execute.cpp
                  physical_limit.cpp
                  physical_pragma.cpp
                  physical_prepare.cpp
                  physical_transaction.cpp
                  physical_vacuum.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES}
                     $<TARGET_OBJECTS:duckdb_operator_helper> PARENT_SCOPE)
