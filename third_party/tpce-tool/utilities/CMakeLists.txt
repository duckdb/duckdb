add_library(tpce_utilities OBJECT
            DateTime.cpp
            EGenVersion.cpp
            Money.cpp
            Random.cpp
            condition.cpp
            error.cpp
            locking.cpp
            threading.cpp)
set(TPCE_OBJECT_FILES
    ${TPCE_OBJECT_FILES} $<TARGET_OBJECTS:tpce_utilities>
    PARENT_SCOPE)

disable_target_warnings(tpce_utilities)
