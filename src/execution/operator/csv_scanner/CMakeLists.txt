add_subdirectory(buffer_manager)
add_subdirectory(scanner)
add_subdirectory(sniffer)
add_subdirectory(state_machine)
add_subdirectory(table_function)
add_subdirectory(util)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES}
    PARENT_SCOPE)
