include_guard(GLOBAL)

function(duckdb_add_windows_static_export_archive CORE_TARGET FILTERED_TARGET STRIP_SCRIPT)
    if(NOT WIN32
       OR NOT MSVC
       OR WINDOWS_STATIC_LIBRARY_EXPORTS
       OR CLANG_TIDY)
        return()
    endif()
    if(CMAKE_VERSION VERSION_LESS "3.20")
        message(
            STATUS "CMake 3.20 or newer is required to remove core exports from Windows extensions; keeping exports")
        return()
    endif()

    get_filename_component(LLVM_TOOLS_DIRECTORY "${CMAKE_CXX_COMPILER}" DIRECTORY)
    find_program(
        LLVM_OBJCOPY_EXECUTABLE
        NAMES llvm-objcopy
        HINTS "${LLVM_TOOLS_DIRECTORY}")
    find_program(
        LLVM_AR_EXECUTABLE
        NAMES llvm-ar
        HINTS "${LLVM_TOOLS_DIRECTORY}")
    if(NOT LLVM_OBJCOPY_EXECUTABLE OR NOT LLVM_AR_EXECUTABLE)
        message(STATUS "llvm-objcopy or llvm-ar not found; keeping core exports in Windows extensions")
        return()
    endif()
    if(NOT Python3_FOUND)
        message(FATAL_ERROR "Removing Windows static exports requires Python3")
    endif()

    # Compile the core once and keep the original archive's host exports.
    set(ARCHIVE_NAME "${FILTERED_TARGET}${CMAKE_STATIC_LIBRARY_SUFFIX}")
    set(ARCHIVE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/$<CONFIG>")
    set(FILTERED_ARCHIVE "${ARCHIVE_DIRECTORY}/${ARCHIVE_NAME}")
    set(STAGED_ARCHIVE "${FILTERED_ARCHIVE}.tmp")
    add_custom_command(
        OUTPUT "${FILTERED_ARCHIVE}"
        COMMAND "${CMAKE_COMMAND}" -E make_directory "${ARCHIVE_DIRECTORY}"
        COMMAND "${CMAKE_COMMAND}" -E copy "$<TARGET_FILE:${CORE_TARGET}>" "${STAGED_ARCHIVE}"
        COMMAND "${Python3_EXECUTABLE}" "${STRIP_SCRIPT}" "${STAGED_ARCHIVE}" --objcopy "${LLVM_OBJCOPY_EXECUTABLE}"
                --ar "${LLVM_AR_EXECUTABLE}" --lib "${CMAKE_AR}"
        COMMAND "${CMAKE_COMMAND}" -E rename "${STAGED_ARCHIVE}" "${FILTERED_ARCHIVE}"
        DEPENDS ${CORE_TARGET} "${STRIP_SCRIPT}"
        COMMENT "Removing core DLL exports from the Windows extension archive with LLVM"
        VERBATIM)
    add_custom_target(${FILTERED_TARGET}_archive DEPENDS "${FILTERED_ARCHIVE}")

    add_library(${FILTERED_TARGET} STATIC IMPORTED GLOBAL)
    set_target_properties(
        ${FILTERED_TARGET}
        PROPERTIES IMPORTED_LOCATION "${CMAKE_CURRENT_BINARY_DIR}/${CMAKE_BUILD_TYPE}/${ARCHIVE_NAME}"
                   INTERFACE_LINK_LIBRARIES "$<TARGET_PROPERTY:${CORE_TARGET},INTERFACE_LINK_LIBRARIES>")
    foreach(CONFIGURATION IN LISTS CMAKE_CONFIGURATION_TYPES)
        string(TOUPPER "${CONFIGURATION}" CONFIGURATION_UPPER)
        set_property(TARGET ${FILTERED_TARGET} PROPERTY IMPORTED_LOCATION_${CONFIGURATION_UPPER}
                                                        "${CMAKE_CURRENT_BINARY_DIR}/${CONFIGURATION}/${ARCHIVE_NAME}")
    endforeach()
    add_dependencies(${FILTERED_TARGET} ${FILTERED_TARGET}_archive)
endfunction()

# Resolve in the final consumer, after core targets have been declared. Static hosts keep the original core archive;
# self-contained Windows loadables use the filtered one.
function(duckdb_link_static_extension_core TARGET LINKAGE)
    target_link_libraries(
        ${TARGET}
        ${LINKAGE}
        "$<$<NOT:$<BOOL:$<TARGET_PROPERTY:DUCKDB_DYNAMIC_LOADABLE_EXTENSION>>>:$<IF:$<AND:$<BOOL:$<TARGET_PROPERTY:DUCKDB_SELF_CONTAINED_WINDOWS_EXTENSION>>,$<TARGET_EXISTS:duckdb_static_no_exports>>,duckdb_static_no_exports,duckdb_static>>"
        ${ARGN})
endfunction()

function(
    build_loadable_extension_directory
    NAME
    ABI_TYPE
    OUTPUT_DIRECTORY
    EXTENSION_VERSION
    CAPI_VERSION
    PARAMETERS)
    set(TARGET_NAME ${NAME}_loadable_extension)
    string(FIND "${PARAMETERS}" "-host-bound" HOST_BOUND)
    if(LOCAL_EXTENSION_REPO)
        add_dependencies(duckdb_local_extension_repo ${NAME}_loadable_extension)
    endif()
    # all parameters after output_directory
    set(FILES "${ARGV}")
    # remove name, abi_type, output_directory, extension_version, capi_version, parameters
    list(
        REMOVE_AT
        FILES
        0
        1
        2
        3
        4
        5)

    # parse parameters
    string(FIND "${PARAMETERS}" "-no-warnings" IGNORE_WARNINGS)

    string(TOUPPER ${NAME} EXTENSION_NAME_UPPERCASE)

    if(EMSCRIPTEN)
        add_library(${TARGET_NAME} STATIC ${FILES})
    else()
        add_library(${TARGET_NAME} SHARED ${FILES})
    endif()
    # this disables the -Dsome_target_EXPORTS define being added by cmake which otherwise trips clang-tidy (yay)
    set_target_properties(${TARGET_NAME} PROPERTIES DEFINE_SYMBOL "")
    set_target_properties(${TARGET_NAME} PROPERTIES OUTPUT_NAME ${NAME})
    set_target_properties(${TARGET_NAME} PROPERTIES PREFIX "")
    if(${HOST_BOUND} GREATER -1 OR NOT EXTENSION_STATIC_BUILD)
        set_property(TARGET ${TARGET_NAME} PROPERTY DUCKDB_DYNAMIC_LOADABLE_EXTENSION TRUE)
    endif()
    if(${IGNORE_WARNINGS} GREATER -1)
        disable_target_warnings(${TARGET_NAME})
    endif()
    # loadable extension binaries can be built two ways: 1. EXTENSION_STATIC_BUILD=1 DuckDB is statically linked into
    # each extension binary. This increases portability because in several situations DuckDB itself may have been loaded
    # with RTLD_LOCAL. This is currently the main way we distribute the loadable extension binaries 2.
    # EXTENSION_STATIC_BUILD=0 The DuckDB symbols required by the loadable extensions are left unresolved. This will
    # reduce the size of the binaries and works well when running the DuckDB cli directly. For windows this uses delay
    # loading. For MacOS and linux the dynamic loader will look up the missing symbols when the extension is dlopen-ed.
    if(WASM_LOADABLE_EXTENSIONS)
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -sSIDE_MODULE=1 -DWASM_LOADABLE_EXTENSIONS")
    elseif(${ABI_TYPE} STREQUAL "C_STRUCT" OR ${ABI_TYPE} STREQUAL "C_STRUCT_UNSTABLE")
        # Export only the entrypoint, so bundled libraries cannot clash with symbols of the host process
        capi_entrypoint_symbol(${NAME} ${ABI_TYPE} "${CAPI_VERSION}" CAPI_ENTRYPOINT)
        if(EMSCRIPTEN
           OR WIN32
           OR ZOS)
            # Wasm only exports EXPORTED_FUNCTIONS, Windows only exports dllexport symbols
        elseif(APPLE)
            target_link_options(${TARGET_NAME} PRIVATE "LINKER:-exported_symbol,_${CAPI_ENTRYPOINT}")
        elseif("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU" OR "${CMAKE_CXX_COMPILER_ID}" MATCHES "Clang")
            set(EXPORTS_MAP "${CMAKE_CURRENT_BINARY_DIR}/${NAME}_loadable_exports.map")
            file(WRITE "${EXPORTS_MAP}" "{ global: ${CAPI_ENTRYPOINT}; local: *; };\n")
            target_link_options(${TARGET_NAME} PRIVATE "LINKER:--version-script=${EXPORTS_MAP}")
            set_property(
                TARGET ${TARGET_NAME}
                APPEND
                PROPERTY LINK_DEPENDS "${EXPORTS_MAP}")
        endif()
    elseif(${HOST_BOUND} GREATER -1)
        if("${CMAKE_CXX_COMPILER_ID}" MATCHES "Clang$" AND APPLE)
            set_target_properties(${TARGET_NAME} PROPERTIES LINK_FLAGS "-undefined dynamic_lookup")
        endif()
    elseif(EXTENSION_STATIC_BUILD)
        if(WIN32)
            set_property(TARGET ${TARGET_NAME} PROPERTY DUCKDB_SELF_CONTAINED_WINDOWS_EXTENSION TRUE)
            duckdb_link_static_extension_core(${TARGET_NAME} "" ${DUCKDB_EXTRA_LINK_FLAGS})
            if(MSVC)
                # Statically linked core APIs need direct references, while DUCKDB_EXTENSION_API still exports the
                # entrypoint.
                target_compile_definitions(${TARGET_NAME} PRIVATE DUCKDB_API=)
                # link.exe/lld-link only default these on without /DEBUG; pin the --gc-sections equivalent
                target_link_options(${TARGET_NAME} PRIVATE "$<$<CONFIG:Release>:/OPT:REF;/OPT:ICF>")
            endif()
        elseif("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU" OR "${CMAKE_CXX_COMPILER_ID}" MATCHES "Clang")
            if(APPLE)
                set_target_properties(${TARGET_NAME} PROPERTIES CXX_VISIBILITY_PRESET hidden)
                # Note that on MacOS we need to use the -exported_symbol whitelist feature due to a lack of
                # -exclude-libs flag in mac's ld variant
                set(WHITELIST "-Wl,-exported_symbol,_${NAME}_duckdb_cpp_init")
                target_link_libraries(${TARGET_NAME} duckdb_static ${DUCKDB_EXTRA_LINK_FLAGS} -Wl,-dead_strip
                                      ${WHITELIST})
            elseif(ZOS)
                target_link_libraries(${TARGET_NAME} duckdb_static ${DUCKDB_EXTRA_LINK_FLAGS})
            else()
                # For GNU we rely on fvisibility=hidden to hide the extension symbols and use -exclude-libs to hide the
                # duckdb symbols
                set_target_properties(${TARGET_NAME} PROPERTIES CXX_VISIBILITY_PRESET hidden)
                if(DUCKDB_LOADABLE_FROM_ARCHIVE)
                    # The entry point comes from an archive too, so --exclude-libs,ALL would hide it; export it alone
                    # through a version script instead.
                    set(EXPORTS_MAP "${CMAKE_CURRENT_BINARY_DIR}/${NAME}_loadable_exports.map")
                    file(WRITE "${EXPORTS_MAP}" "{ global: ${NAME}_duckdb_cpp_init; local: *; };\n")
                    target_link_libraries(${TARGET_NAME} duckdb_static ${DUCKDB_EXTRA_LINK_FLAGS} -Wl,--gc-sections
                                          -Wl,--version-script=${EXPORTS_MAP})
                    set_property(
                        TARGET ${TARGET_NAME}
                        APPEND
                        PROPERTY LINK_DEPENDS "${EXPORTS_MAP}")
                else()
                    target_link_libraries(${TARGET_NAME} duckdb_static ${DUCKDB_EXTRA_LINK_FLAGS} -Wl,--gc-sections
                                          -Wl,--exclude-libs,ALL)
                endif()
            endif()
        else()
            message(FATAL_ERROR "EXTENSION static build is only intended for Linux and Windows on MVSC")
        endif()
    else()
        if(WIN32)
            target_link_libraries(${TARGET_NAME} duckdb ${DUCKDB_EXTRA_LINK_FLAGS})
        elseif("${CMAKE_CXX_COMPILER_ID}" MATCHES "Clang$")
            if(APPLE)
                set_target_properties(${TARGET_NAME} PROPERTIES LINK_FLAGS "-undefined dynamic_lookup")
            endif()
        endif()
    endif()
    if(MSVC)
        target_compile_options(${TARGET_NAME} PRIVATE /UDUCKDB_BUILD_LIBRARY)
    else()
        target_compile_options(${TARGET_NAME} PRIVATE -UDUCKDB_BUILD_LIBRARY)
    endif()
    set_target_properties(${TARGET_NAME} PROPERTIES SUFFIX ".duckdb_extension")

    if(MSVC)
        set_target_properties(${TARGET_NAME} PROPERTIES RUNTIME_OUTPUT_DIRECTORY_DEBUG
                                                        "${CMAKE_BINARY_DIR}/${OUTPUT_DIRECTORY}")
        set_target_properties(${TARGET_NAME} PROPERTIES RUNTIME_OUTPUT_DIRECTORY_RELEASE
                                                        "${CMAKE_BINARY_DIR}/${OUTPUT_DIRECTORY}")
    endif()

    if(EMSCRIPTEN)
        # Compile the library into the actual wasm file
        string(TOUPPER ${NAME} EXTENSION_NAME_UPPERCASE)
        set(TO_BE_LINKED ${DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_LINKED_LIBS})
        separate_arguments(TO_BE_LINKED)
        if(${ABI_TYPE} STREQUAL "CPP")
            set(EXPORTED_FUNCTIONS "_${NAME}_duckdb_cpp_init")
        else()
            capi_entrypoint_symbol(${NAME} ${ABI_TYPE} "${CAPI_VERSION}" CAPI_ENTRYPOINT)
            set(EXPORTED_FUNCTIONS "_${CAPI_ENTRYPOINT}")
        endif()
        add_custom_command(
            TARGET ${TARGET_NAME}
            POST_BUILD
            COMMAND emcc $<TARGET_FILE:${TARGET_NAME}> -o $<TARGET_FILE:${TARGET_NAME}>.wasm -O3 -sSIDE_MODULE=2
                    -sEXPORTED_FUNCTIONS="${EXPORTED_FUNCTIONS}" ${WASM_THREAD_FLAGS} ${TO_BE_LINKED})
    endif()

    if(${ABI_TYPE} STREQUAL "CPP")
        set(FOOTER_VERSION_VALUE ${DUCKDB_NORMALIZED_VERSION})
    elseif(${ABI_TYPE} STREQUAL "C_STRUCT_UNSTABLE")
        set(FOOTER_VERSION_VALUE ${DUCKDB_NORMALIZED_VERSION})
    elseif(${ABI_TYPE} STREQUAL "C_STRUCT")
        set(FOOTER_VERSION_VALUE ${CAPI_VERSION})
    endif()

    add_custom_command(
        TARGET ${TARGET_NAME}
        POST_BUILD
        COMMAND
            ${CMAKE_COMMAND} -DABI_TYPE=${ABI_TYPE} -DEXTENSION=$<TARGET_FILE:${TARGET_NAME}>${EXTENSION_POSTFIX}
            -DPLATFORM_FILE=${DuckDB_BINARY_DIR}/duckdb_platform_out -DVERSION_FIELD="${FOOTER_VERSION_VALUE}"
            -DEXTENSION_VERSION="${EXTENSION_VERSION}" -DNULL_FILE=${DUCKDB_MODULE_BASE_DIR}/scripts/null.txt -P
            ${DUCKDB_MODULE_BASE_DIR}/scripts/append_metadata.cmake)
    add_dependencies(${TARGET_NAME} duckdb_platform)
    if(NOT ${EXTENSION_CONFIG_BUILD}
       AND NOT ${EXTENSION_TESTS_ONLY}
       AND NOT CLANG_TIDY)
        add_dependencies(duckdb_local_extension_repo ${TARGET_NAME})
    endif()
endfunction()

function(build_static_extension NAME PARAMETERS)
    # all parameters after name
    set(FILES "${ARGV}")
    list(REMOVE_AT FILES 0)
    string(TOUPPER ${NAME} EXTENSION_NAME_UPPERCASE)
    set(PREBUILT_PATH "${DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_PREBUILT_PATH}")
    if(NOT "${PREBUILT_PATH}" STREQUAL "")
        set(DUMMY_SOURCE "${DuckDB_BINARY_DIR}/codegen/prebuilt_extension.cpp")
        if(NOT EXISTS "${DUMMY_SOURCE}")
            file(WRITE "${DUMMY_SOURCE}" "// Wrapper for prebuilt extension archives.\n")
        endif()
        # Keep the DuckDB archive after the extension archive for one-pass linkers.
        set(PREBUILT_TARGET ${NAME}_prebuilt_extension)
        add_library(${PREBUILT_TARGET} STATIC IMPORTED GLOBAL)
        set_property(TARGET ${PREBUILT_TARGET} PROPERTY IMPORTED_LOCATION "${PREBUILT_PATH}")
        duckdb_link_static_extension_core(${PREBUILT_TARGET} INTERFACE)
        add_library(${NAME}_extension STATIC "${DUMMY_SOURCE}")
        target_link_libraries(${NAME}_extension "$<BUILD_INTERFACE:${PREBUILT_TARGET}>")
        set_property(TARGET ${NAME}_extension PROPERTY DUCKDB_EXTENSION_KIND CPP)
        set_property(TARGET ${NAME}_extension PROPERTY DUCKDB_PREBUILT_EXTENSION TRUE)
        return()
    endif()
    add_library(${NAME}_extension STATIC ${FILES})
    # Dynamic loadables resolve DuckDB symbols from the host, including when linked through this archive.
    duckdb_link_static_extension_core(${NAME}_extension "")
    duckdb_add_extension_describe(${NAME} CPP)
    duckdb_make_native_lto_archive(${NAME}_extension)
endfunction()

# Compiles a C++ extension once into lib<NAME>_extension.a and links its loadable from that archive. NO_LOADABLE builds
# the archive only; NO_WARNINGS silences compiler warnings.
function(build_extension_library NAME)
    cmake_parse_arguments(PARSE_ARGV 1 ARG "NO_LOADABLE;NO_WARNINGS;DEFAULT_VISIBILITY;HOST_BOUND" "" "")
    set(FILES ${ARG_UNPARSED_ARGUMENTS})
    set(PARAMETERS "-warnings")
    if(ARG_NO_WARNINGS)
        set(PARAMETERS "-no-warnings")
    endif()
    if(ARG_HOST_BOUND AND NOT WIN32)
        string(APPEND PARAMETERS " -host-bound")
    endif()

    build_static_extension(${NAME} ${FILES})
    if(ARG_NO_WARNINGS)
        disable_target_warnings(${NAME}_extension)
    endif()
    if(ARG_NO_LOADABLE)
        return()
    endif()
    if(EMSCRIPTEN OR WASM_LOADABLE_EXTENSIONS)
        # the wasm side module is produced from the loadable target's own objects
        build_loadable_extension(${NAME} "${PARAMETERS}" ${FILES})
        return()
    endif()

    # the loadable exports only its entry point either way; DEFAULT_VISIBILITY keeps internals visible through libduckdb
    if(NOT ARG_DEFAULT_VISIBILITY)
        set_target_properties(${NAME}_extension PROPERTIES CXX_VISIBILITY_PRESET hidden)
    endif()
    set(LOADABLE_SOURCE "${DuckDB_BINARY_DIR}/codegen/loadable_from_archive.cpp")
    if(NOT EXISTS "${LOADABLE_SOURCE}")
        file(WRITE "${LOADABLE_SOURCE}"
             "// A loadable extension built by build_extension_library takes its code from its static archive.\n")
    endif()
    set(DUCKDB_LOADABLE_FROM_ARCHIVE TRUE)
    string(TOUPPER ${NAME} EXTENSION_NAME_UPPERCASE)
    build_loadable_extension_directory(
        ${NAME}
        "CPP"
        "extension/${NAME}"
        "${DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_EXT_VERSION}"
        ""
        "${PARAMETERS}"
        ${LOADABLE_SOURCE})
    target_link_libraries(${NAME}_loadable_extension ${NAME}_extension)
    # the entrypoint lives in the archive, so the link has to ask for it
    set(ENTRY ${NAME}_duckdb_cpp_init)
    if(MSVC)
        if(CMAKE_SIZEOF_VOID_P EQUAL 4)
            set(ENTRY "_${ENTRY}")
        endif()
        target_link_options(${NAME}_loadable_extension PRIVATE "/INCLUDE:${ENTRY}")
    elseif(APPLE)
        target_link_options(${NAME}_loadable_extension PRIVATE "-Wl,-u,_${ENTRY}")
    else()
        if(MINGW AND CMAKE_SIZEOF_VOID_P EQUAL 4)
            set(ENTRY "_${ENTRY}")
        endif()
        target_link_options(${NAME}_loadable_extension PRIVATE "-Wl,-u,${ENTRY}")
    endif()
endfunction()
