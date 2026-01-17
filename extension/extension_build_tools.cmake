
function(add_extension_definitions)
    include_directories(${PROJECT_SOURCE_DIR}/extension)
    if(NOT "${TEST_WITH_LOADABLE_EXTENSION}" STREQUAL "")
        string(REPLACE ";"  "," COMMA_SEPARATED_EXTENSIONS "${TEST_WITH_LOADABLE_EXTENSION}")
        # Note: weird commas are for easy substring matching in c++
        add_definitions(-DDUCKDB_EXTENSIONS_TEST_WITH_LOADABLE=\",${COMMA_SEPARATED_EXTENSIONS},\")
        add_definitions(-DDUCKDB_EXTENSIONS_BUILD_PATH="${CMAKE_BINARY_DIR}/extension")
    endif()

    if(${DISABLE_BUILTIN_EXTENSIONS})
        add_definitions(-DDISABLE_BUILTIN_EXTENSIONS=${DISABLE_BUILTIN_EXTENSIONS})
    endif()

    # Include paths for any registered out-of-tree extensions
    foreach(EXT_NAME IN LISTS DUCKDB_EXTENSION_NAMES)
        string(TOUPPER ${EXT_NAME} EXT_NAME_UPPERCASE)
        if(${DUCKDB_EXTENSION_${EXT_NAME_UPPERCASE}_SHOULD_LINK})
            add_definitions(-DDUCKDB_EXTENSION_${EXT_NAME_UPPERCASE}_LINKED=1)
            if (DEFINED DUCKDB_EXTENSION_${EXT_NAME_UPPERCASE}_INCLUDE_PATH)
                include_directories("${DUCKDB_EXTENSION_${EXT_NAME_UPPERCASE}_INCLUDE_PATH}")
            else()
                # We try the default locations for headers
                include_directories("${PROJECT_SOURCE_DIR}/extension_external/${EXT_NAME}/src/include")
                include_directories("${PROJECT_SOURCE_DIR}/extension_external/${EXT_NAME}/include")
            endif()
        endif()
    endforeach()
endfunction()

function(add_extension_dependencies LIBRARY)
    foreach(EXT_NAME IN LISTS DUCKDB_EXTENSION_NAMES)
        string(TOUPPER ${EXT_NAME} EXTENSION_NAME_UPPERCASE)
        if (DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_SHOULD_LINK)
            add_dependencies(${LIBRARY} ${EXT_NAME}_extension)
        endif()
    endforeach()
endfunction()

function(get_statically_linked_extensions DUCKDB_EXTENSION_NAMES OUT_VARIABLE)
    if(NOT ${DISABLE_BUILTIN_EXTENSIONS})
        set(${OUT_VARIABLE} ${DUCKDB_EXTENSION_NAMES} PARENT_SCOPE)
    elseif(${GENERATE_EXTENSION_ENTRIES})
        set(${OUT_VARIABLE} "" PARENT_SCOPE)
    else()
        set(${OUT_VARIABLE} "" PARENT_SCOPE)
        foreach(EXT_NAME IN LISTS DUCKDB_EXTENSION_NAMES)
            if(${EXT_NAME} STREQUAL "core_functions")
                set(${OUT_VARIABLE} "core_functions" PARENT_SCOPE)
            endif()
        endforeach()
    endif()
endfunction()

function(link_extension_libraries LIBRARY LINKAGE)
    get_statically_linked_extensions("${DUCKDB_EXTENSION_NAMES}" STATICALLY_LINKED_EXTENSIONS)
    # Now link against any registered out-of-tree extensions
    foreach(EXT_NAME IN LISTS STATICALLY_LINKED_EXTENSIONS)
        string(TOUPPER ${EXT_NAME} EXT_NAME_UPPERCASE)
        if (${DUCKDB_EXTENSION_${EXT_NAME_UPPERCASE}_SHOULD_LINK})
            target_link_libraries(${LIBRARY} ${LINKAGE} ${EXT_NAME}_extension)
        endif()
    endforeach()
    target_link_libraries(${LIBRARY} ${LINKAGE} duckdb_generated_extension_loader)
endfunction()

function(link_threads LIBRARY LINKAGE)
    target_link_libraries(${LIBRARY} ${LINKAGE} Threads::Threads)
endfunction()

# Deploys extensions to a local repository (a folder structure that contains the duckdb version + binary arch)
if ("${LOCAL_EXTENSION_REPO}" STREQUAL "")
    set(LOCAL_EXTENSION_REPO_DIR ${CMAKE_BINARY_DIR}/repository)
else()
    if (NOT Python3_FOUND)
        MESSAGE(FATAL_ERROR "Could not find python3 executable, when providing LOCAL_EXTENSION_REPO this is compulsory")
    endif()
    set(LOCAL_EXTENSION_REPO_DIR ${LOCAL_EXTENSION_REPO})
endif()

set(LOCAL_EXTENSION_REPO FALSE)
if (NOT ${EXTENSION_CONFIG_BUILD} AND NOT ${EXTENSION_TESTS_ONLY} AND NOT CLANG_TIDY)
    if (NOT Python3_FOUND)
        add_custom_target(
                duckdb_local_extension_repo ALL)
        MESSAGE(STATUS "Could not find python3, create extension directory step will be skipped")
    else()
        add_custom_target(
                duckdb_local_extension_repo ALL
                COMMAND
                ${Python3_EXECUTABLE} scripts/create_local_extension_repo.py "${DUCKDB_NORMALIZED_VERSION}" "${CMAKE_CURRENT_BINARY_DIR}/duckdb_platform_out" "${CMAKE_CURRENT_BINARY_DIR}" "${LOCAL_EXTENSION_REPO_DIR}" "duckdb_extension${EXTENSION_POSTFIX}"
                WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
                COMMENT Create local extension repository)
        add_dependencies(duckdb_local_extension_repo duckdb_platform)
        set(LOCAL_EXTENSION_REPO TRUE)
        message(STATUS "Extensions will be deployed to: ${LOCAL_EXTENSION_REPO_DIR}")
    endif()
endif()

function(build_loadable_extension_directory NAME ABI_TYPE OUTPUT_DIRECTORY EXTENSION_VERSION CAPI_VERSION PARAMETERS)
    set(TARGET_NAME ${NAME}_loadable_extension)
    if (LOCAL_EXTENSION_REPO)
        add_dependencies(duckdb_local_extension_repo ${NAME}_loadable_extension)
    endif()
    # all parameters after output_directory
    set(FILES "${ARGV}")
    # remove name, abi_type, output_directory, extension_version, capi_version, parameters
    list(REMOVE_AT FILES 0 1 2 3 4 5)

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
    if(${IGNORE_WARNINGS} GREATER -1)
        disable_target_warnings(${TARGET_NAME})
    endif()
    # loadable extension binaries can be built two ways:
    # 1. EXTENSION_STATIC_BUILD=1
    #    DuckDB is statically linked into each extension binary. This increases portability because in several situations
    #    DuckDB itself may have been loaded with RTLD_LOCAL. This is currently the main way we distribute the loadable
    #    extension binaries
    # 2. EXTENSION_STATIC_BUILD=0
    #    The DuckDB symbols required by the loadable extensions are left unresolved. This will reduce the size of the binaries
    #    and works well when running the DuckDB cli directly. For windows this uses delay loading. For MacOS and linux the
    #    dynamic loader will look up the missing symbols when the extension is dlopen-ed.
    if(WASM_LOADABLE_EXTENSIONS)
        set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -sSIDE_MODULE=1 -DWASM_LOADABLE_EXTENSIONS")
    elseif(${ABI_TYPE} STREQUAL "C_STRUCT" OR ${ABI_TYPE} STREQUAL "C_STRUCT_UNSTABLE")
        # TODO strip all symbols except the capi init
    elseif (EXTENSION_STATIC_BUILD)
        if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU" OR "${CMAKE_CXX_COMPILER_ID}" MATCHES "Clang")
            if (APPLE)
                set_target_properties(${TARGET_NAME} PROPERTIES CXX_VISIBILITY_PRESET hidden)
                # Note that on MacOS we need to use the -exported_symbol whitelist feature due to a lack of -exclude-libs flag in mac's ld variant
                set(WHITELIST "-Wl,-exported_symbol,_${NAME}_duckdb_cpp_init")
                target_link_libraries(${TARGET_NAME} duckdb_static dummy_static_extension_loader ${DUCKDB_EXTRA_LINK_FLAGS} -Wl,-dead_strip ${WHITELIST})
            elseif (ZOS)
                target_link_libraries(${TARGET_NAME} duckdb_static ${DUCKDB_EXTRA_LINK_FLAGS})
            else()
                if (MSVC_VERSION)
                    # MSVC + Clang GNU CLI build combo
                    target_link_libraries(${TARGET_NAME} duckdb_static dummy_static_extension_loader ${DUCKDB_EXTRA_LINK_FLAGS})
                else()
                    # For GNU we rely on fvisibility=hidden to hide the extension symbols and use -exclude-libs to hide the duckdb symbols
                    set_target_properties(${TARGET_NAME} PROPERTIES CXX_VISIBILITY_PRESET hidden)
                    target_link_libraries(${TARGET_NAME} duckdb_static dummy_static_extension_loader ${DUCKDB_EXTRA_LINK_FLAGS} -Wl,--gc-sections -Wl,--exclude-libs,ALL)
                endif()
            endif()
        elseif (WIN32)
            target_link_libraries(${TARGET_NAME} duckdb_static dummy_static_extension_loader ${DUCKDB_EXTRA_LINK_FLAGS})
        else()
            message(FATAL_ERROR, "EXTENSION static build is only intended for Linux and Windows on MVSC")
        endif()
    else()
        if (WIN32)
            target_link_libraries(${TARGET_NAME} duckdb ${DUCKDB_EXTRA_LINK_FLAGS})
        elseif("${CMAKE_CXX_COMPILER_ID}" MATCHES "Clang$")
            if (APPLE)
                set_target_properties(${TARGET_NAME} PROPERTIES LINK_FLAGS "-undefined dynamic_lookup")
            endif()
        endif()
    endif()


    target_compile_definitions(${TARGET_NAME} PUBLIC -DDUCKDB_BUILD_LOADABLE_EXTENSION)
    set_target_properties(${TARGET_NAME} PROPERTIES SUFFIX
            ".duckdb_extension")

    if(MSVC)
        set_target_properties(
                ${TARGET_NAME} PROPERTIES RUNTIME_OUTPUT_DIRECTORY_DEBUG
                "${CMAKE_BINARY_DIR}/${OUTPUT_DIRECTORY}")
        set_target_properties(
                ${TARGET_NAME} PROPERTIES RUNTIME_OUTPUT_DIRECTORY_RELEASE
                "${CMAKE_BINARY_DIR}/${OUTPUT_DIRECTORY}")
    endif()

    if(EMSCRIPTEN)
        # Compile the library into the actual wasm file
        string(TOUPPER ${NAME} EXTENSION_NAME_UPPERCASE)
        set(TO_BE_LINKED ${DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_LINKED_LIBS} )
        separate_arguments(TO_BE_LINKED)
        if (${ABI_TYPE} STREQUAL "CPP")
            set(EXPORTED_FUNCTIONS "_${NAME}_duckdb_cpp_init")
        elseif (${ABI_TYPE} STREQUAL "C_STRUCT" OR ${ABI_TYPE} STREQUAL "C_STRUCT_UNSTABLE")
            set(EXPORTED_FUNCTIONS "_${NAME}_init_c_api")
        endif()
        add_custom_command(
                TARGET ${TARGET_NAME}
                POST_BUILD
                COMMAND emcc $<TARGET_FILE:${TARGET_NAME}> -o $<TARGET_FILE:${TARGET_NAME}>.wasm -O3 -sSIDE_MODULE=2 -sEXPORTED_FUNCTIONS="${EXPORTED_FUNCTIONS}" ${WASM_THREAD_FLAGS} ${TO_BE_LINKED}
        )
    endif()

    if (${ABI_TYPE} STREQUAL "CPP")
        set(FOOTER_VERSION_VALUE ${DUCKDB_NORMALIZED_VERSION})
    elseif (${ABI_TYPE} STREQUAL "C_STRUCT_UNSTABLE")
        set(FOOTER_VERSION_VALUE ${DUCKDB_NORMALIZED_VERSION})
    elseif (${ABI_TYPE} STREQUAL "C_STRUCT")
        set(FOOTER_VERSION_VALUE ${CAPI_VERSION})
    endif()

    add_custom_command(
            TARGET ${TARGET_NAME}
            POST_BUILD
            COMMAND
            ${CMAKE_COMMAND} -DABI_TYPE=${ABI_TYPE} -DEXTENSION=$<TARGET_FILE:${TARGET_NAME}>${EXTENSION_POSTFIX} -DPLATFORM_FILE=${DuckDB_BINARY_DIR}/duckdb_platform_out -DVERSION_FIELD="${FOOTER_VERSION_VALUE}" -DEXTENSION_VERSION="${EXTENSION_VERSION}" -DNULL_FILE=${DUCKDB_MODULE_BASE_DIR}/scripts/null.txt -P ${DUCKDB_MODULE_BASE_DIR}/scripts/append_metadata.cmake
    )
    add_dependencies(${TARGET_NAME} duckdb_platform)
    if (NOT ${EXTENSION_CONFIG_BUILD} AND NOT ${EXTENSION_TESTS_ONLY} AND NOT CLANG_TIDY)
        add_dependencies(duckdb_local_extension_repo ${TARGET_NAME})
    endif()
endfunction()

function(build_loadable_extension NAME PARAMETERS)
    # all parameters after name
    set(FILES "${ARGV}")
    list(REMOVE_AT FILES 0 1)
    string(TOUPPER ${NAME} EXTENSION_NAME_UPPERCASE)

    build_loadable_extension_directory(${NAME} "CPP" "extension/${NAME}" "${DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_EXT_VERSION}" "" "${PARAMETERS}" ${FILES})
endfunction()

function(build_loadable_extension_capi NAME CAPI_VERSION_MAJOR CAPI_VERSION_MINOR CAPI_VERSION_PATCH PARAMETERS)
    set(FILES "${ARGV}")
    list(REMOVE_AT FILES 0 1 2 3)
    set(CAPI_VERSION v${CAPI_VERSION_MAJOR}.${CAPI_VERSION_MINOR}.${CAPI_VERSION_PATCH})
    build_loadable_extension_capi_internal(${NAME} ${CAPI_VERSION} "C_STRUCT" ${FILES})
    target_compile_definitions(${NAME}_loadable_extension PRIVATE DUCKDB_EXTENSION_API_VERSION_MAJOR=${CAPI_VERSION_MAJOR})
    target_compile_definitions(${NAME}_loadable_extension PRIVATE DUCKDB_EXTENSION_API_VERSION_MINOR=${CAPI_VERSION_MINOR})
    target_compile_definitions(${NAME}_loadable_extension PRIVATE DUCKDB_EXTENSION_API_VERSION_PATCH=${CAPI_VERSION_PATCH})
    target_compile_definitions(${NAME}_loadable_extension PRIVATE DUCKDB_EXTENSION_NAME=${NAME})
endfunction()

function(build_loadable_extension_capi_unstable NAME PARAMETERS)
    set(FILES "${ARGV}")
    list(REMOVE_AT FILES 0)
    build_loadable_extension_capi_internal(${NAME} "" "C_STRUCT_UNSTABLE" ${FILES})
    target_compile_definitions(${NAME}_loadable_extension PRIVATE DUCKDB_EXTENSION_API_VERSION_UNSTABLE=${DUCKDB_NORMALIZED_VERSION})
    target_compile_definitions(${NAME}_loadable_extension PRIVATE DUCKDB_EXTENSION_NAME=${NAME})
endfunction()

function(build_loadable_extension_capi_internal NAME VERSION ABI_TYPE PARAMETERS)
    # all parameters after name
    set(FILES "${ARGV}")
    list(REMOVE_AT FILES 0 1 2)
    string(TOUPPER ${NAME} EXTENSION_NAME_UPPERCASE)

    build_loadable_extension_directory(${NAME} ${ABI_TYPE} "extension/${NAME}" "${DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_EXT_VERSION}" "${VERSION}" "${PARAMETERS}" ${FILES})
endfunction()

function(build_static_extension NAME PARAMETERS)
    # all parameters after name
    set(FILES "${ARGV}")
    list(REMOVE_AT FILES 0)
    add_library(${NAME}_extension STATIC ${FILES})
    target_link_libraries(${NAME}_extension duckdb_static)
endfunction()

# Internal extension register function
function(register_extension NAME DONT_LINK DONT_BUILD LOAD_TESTS PATH INCLUDE_PATH TEST_PATH LINKED_LIBS EXTENSION_VERSION)
    string(TOLOWER ${NAME} EXTENSION_NAME_LOWERCASE)
    string(TOUPPER ${NAME} EXTENSION_NAME_UPPERCASE)

    set(DUCKDB_EXTENSION_NAMES ${DUCKDB_EXTENSION_NAMES} ${EXTENSION_NAME_LOWERCASE} PARENT_SCOPE)

    if ("${LOAD_TESTS}")
        set(DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_LOAD_TESTS TRUE PARENT_SCOPE)
    else()
        set(DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_LOAD_TESTS FALSE PARENT_SCOPE)
    endif()
    set(LINK_EXTENSION TRUE)
    if (NOT ${BUILD_EXTENSIONS_ONLY})
        if (${DONT_LINK})
            set(LINK_EXTENSION FALSE)
        endif()
        if(DISABLE_BUILTIN_EXTENSIONS)
            if(${GENERATE_EXTENSION_ENTRIES})
                set(LINK_EXTENSION FALSE)
            elseif(${EXTENSION_NAME_UPPERCASE} STREQUAL "CORE_FUNCTIONS")
            else()
                set(LINK_EXTENSION FALSE)
            endif()
        endif()
    endif()
    set(DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_SHOULD_LINK ${LINK_EXTENSION} PARENT_SCOPE)

    set(DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_LINKED_LIBS "${LINKED_LIBS}" PARENT_SCOPE)

    # Allows explicitly disabling extensions that may be specified in other configurations
    if (NOT ${DONT_BUILD} AND NOT ${EXTENSION_TESTS_ONLY})
        set(DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_SHOULD_BUILD TRUE PARENT_SCOPE)
    elseif(NOT ${GENERATE_EXTENSION_ENTRIES} AND ${EXTENSION_NAME_UPPERCASE} STREQUAL "CORE_FUNCTIONS")
        set(DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_SHOULD_BUILD TRUE PARENT_SCOPE)
    else()
        set(DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_SHOULD_BUILD FALSE PARENT_SCOPE)
        set(DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_SHOULD_LINK FALSE PARENT_SCOPE)
    endif()

    if ("${PATH}" STREQUAL "")
        message(FATAL_ERROR "Invalid path set for extension '${NAME}' : '${INCLUDE}'")
    endif()
    if ("${INCLUDE_PATH}" STREQUAL "")
        message(FATAL_ERROR "Invalid include path for extension '${NAME}' : '${INCLUDE_PATH}'")
    endif()
    if ("${TEST_PATH}" STREQUAL "" AND "${LOAD_TESTS}")
        message(FATAL_ERROR "Invalid include path for extension '${NAME}' : '${INCLUDE_PATH}'")
    endif()

    set(DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_PATH ${PATH} PARENT_SCOPE)
    set(DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_INCLUDE_PATH ${INCLUDE_PATH} PARENT_SCOPE)
    set(DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_TEST_PATH ${TEST_PATH} PARENT_SCOPE)
    set(DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_EXT_VERSION ${EXTENSION_VERSION} PARENT_SCOPE)
endfunction()

# Downloads the external extension repo at the specified commit and calls register_extension
macro(register_external_extension NAME URL COMMIT DONT_LINK DONT_BUILD LOAD_TESTS PATH INCLUDE_PATH TEST_PATH APPLY_PATCHES LINKED_LIBS SUBMODULES EXTENSION_VERSION)
    include(FetchContent)

    string(TOUPPER "DUCKDB_${NAME}_DIRECTORY" DIRECTORY_OVERRIDE)
    if(DEFINED ENV{${DIRECTORY_OVERRIDE}})
        set("${NAME}_extension_fc_SOURCE_DIR" "$ENV{${DIRECTORY_OVERRIDE}}")
        message(STATUS "Load extension '${NAME}' from local path \"${${NAME}_extension_fc_SOURCE_DIR}\"")
    else()
        if (${APPLY_PATCHES})
            set(PATCH_COMMAND ${Python3_EXECUTABLE} ${CMAKE_SOURCE_DIR}/scripts/apply_extension_patches.py ${CMAKE_SOURCE_DIR}/.github/patches/extensions/${NAME}/)
        endif()
        FETCHCONTENT_DECLARE(
                ${NAME}_extension_fc
                GIT_REPOSITORY ${URL}
                GIT_TAG ${COMMIT}
                GIT_SUBMODULES "${SUBMODULES}"
                PATCH_COMMAND ${PATCH_COMMAND}
        )
        FETCHCONTENT_POPULATE(${NAME}_EXTENSION_FC)
        message(STATUS "Load extension '${NAME}' from ${URL} @ ${EXTERNAL_EXTENSION_VERSION}")
    endif()

    # Autogenerate version tag if not provided
    if ("${EXTENSION_VERSION}" STREQUAL "")
        duckdb_extension_generate_version(EXTERNAL_EXTENSION_VERSION ${${NAME}_extension_fc_SOURCE_DIR})
    else()
        set(EXTERNAL_EXTENSION_VERSION "${EXTENSION_VERSION}")
    endif()

    string(TOUPPER ${NAME} EXTENSION_NAME_UPPERCASE)
    set(DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_EXT_VERSION "${EXTERNAL_EXTENSION_VERSION}" PARENT_SCOPE)

    if ("${INCLUDE_PATH}" STREQUAL "")
        set(INCLUDE_FULL_PATH "${${NAME}_extension_fc_SOURCE_DIR}/src/include")
    else()
        set(INCLUDE_FULL_PATH "${${NAME}_extension_fc_SOURCE_DIR}/${INCLUDE_PATH}")
    endif()

    if ("${TEST_PATH}" STREQUAL "")
        set(TEST_FULL_PATH "${${NAME}_extension_fc_SOURCE_DIR}/test/sql")
    else()
        set(TEST_FULL_PATH "${${NAME}_extension_fc_SOURCE_DIR}/${TEST_PATH}")
    endif()

    register_extension(${NAME} ${DONT_LINK} ${DONT_BUILD} ${LOAD_TESTS} ${${NAME}_extension_fc_SOURCE_DIR}/${PATH} "${INCLUDE_FULL_PATH}" "${TEST_FULL_PATH}" "${LINKED_LIBS}" "${EXTERNAL_EXTENSION_VERSION}")
endmacro()

# This function sets OUTPUT_VAR to the VERSION using DuckDB's standard versioning convention (using WORKING_DIR)
# TODO: unify this with the base DuckDB logic (note that this is slightly different as it has ReleaseCandidate tag support
function(duckdb_extension_generate_version OUTPUT_VAR WORKING_DIR)
    find_package(Git)
    if(Git_FOUND)
        execute_process(
                COMMAND ${GIT_EXECUTABLE} rev-parse --is-inside-work-tree
                WORKING_DIRECTORY ${WORKING_DIR}
                OUTPUT_VARIABLE IS_IN_GIT_DIR
                ERROR_QUIET
        )
    endif()
    if (IS_IN_GIT_DIR)
        execute_process(
                COMMAND ${GIT_EXECUTABLE} log -1 --format=%h
                WORKING_DIRECTORY ${WORKING_DIR}
                RESULT_VARIABLE GIT_RESULT
                OUTPUT_VARIABLE GIT_COMMIT_HASH
                OUTPUT_STRIP_TRAILING_WHITESPACE)
        if (GIT_RESULT)
            message(FATAL_ERROR "git is available (at ${GIT_EXECUTABLE}) but has failed to execute 'log -1 --format=%h'.")
        endif()
        execute_process(
                COMMAND ${GIT_EXECUTABLE} describe --tags --always --match '${VERSIONING_TAG_MATCH}'
                WORKING_DIRECTORY ${WORKING_DIR}
                RESULT_VARIABLE GIT_RESULT
                OUTPUT_VARIABLE GIT_DESCRIBE
                OUTPUT_STRIP_TRAILING_WHITESPACE)
        if (GIT_RESULT)
            set(VERSION "${GIT_COMMIT_HASH}")
        elseif (GIT_DESCRIBE MATCHES "^v[0-9]+\.[0-9]+\.[0-9]+(-[a-zA-Z0-9\.]+)?$")
            # We are on a valid SemVer version in the format v{MAJOR}.{MINOR}.{PATH}(-{RC})
            set(VERSION "${GIT_DESCRIBE}")
        else()
            set(VERSION "${GIT_COMMIT_HASH}")
        endif()
    else()
        # No git found, we set empty string
        set(VERSION "")
    endif()

    # Propagate the version
    set(${OUTPUT_VAR} ${VERSION} PARENT_SCOPE)
endfunction()

function(duckdb_extension_load NAME)
    # Parameter parsing
    set(options DONT_LINK DONT_BUILD LOAD_TESTS APPLY_PATCHES)
    set(oneValueArgs SOURCE_DIR INCLUDE_DIR TEST_DIR GIT_URL GIT_TAG SUBMODULES EXTENSION_VERSION LINKED_LIBS)
    cmake_parse_arguments(duckdb_extension_load "${options}" "${oneValueArgs}" "" ${ARGN})

    string(TOLOWER ${NAME} EXTENSION_NAME_LOWERCASE)
    string(TOUPPER ${NAME} EXTENSION_NAME_UPPERCASE)

    # If extension was set already, we ignore subsequent calls
    list (FIND DUCKDB_EXTENSION_NAMES ${EXTENSION_NAME_LOWERCASE} _index)
    if (${_index} GREATER -1)
        return()
    endif()

    list (FIND SKIP_EXTENSIONS ${EXTENSION_NAME_LOWERCASE} _index)
    if (${_index} GREATER -1)
        return()
    endif()

    # Remote Git extension
    if (${duckdb_extension_load_DONT_BUILD})
        register_extension(${NAME} "${duckdb_extension_load_DONT_LINK}" "${duckdb_extension_load_DONT_BUILD}" "" "" "" "" "" "${duckdb_extension_load_EXTENSION_VERSION}")
    elseif (NOT "${duckdb_extension_load_GIT_URL}" STREQUAL "")
        if ("${duckdb_extension_load_GIT_TAG}" STREQUAL "")
            message(FATAL_ERROR, "Git URL specified but no valid GIT_TAG was found for ${NAME} extension")
        endif()
        register_external_extension(${NAME} "${duckdb_extension_load_GIT_URL}" "${duckdb_extension_load_GIT_TAG}" "${duckdb_extension_load_DONT_LINK}" "${duckdb_extension_load_DONT_BUILD}" "${duckdb_extension_load_LOAD_TESTS}" "${duckdb_extension_load_SOURCE_DIR}" "${duckdb_extension_load_INCLUDE_DIR}" "${duckdb_extension_load_TEST_DIR}" "${duckdb_extension_load_APPLY_PATCHES}" "${duckdb_extension_load_LINKED_LIBS}" "${duckdb_extension_load_SUBMODULES}" "${duckdb_extension_load_EXTENSION_VERSION}")
        if (NOT "${duckdb_extension_load_EXTENSION_VERSION}" STREQUAL "")
            set(DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_EXT_VERSION "${duckdb_extension_load_EXTENSION_VERSION}" PARENT_SCOPE)
        endif()
    elseif (NOT "${duckdb_extension_load_SOURCE_DIR}" STREQUAL "")
        # Version detection
        if ("${duckdb_extension_load_EXTENSION_VERSION}" STREQUAL "")
            duckdb_extension_generate_version(EXT_VERSION ${duckdb_extension_load_SOURCE_DIR})
        else()
            set(EXT_VERSION ${duckdb_extension_load_EXTENSION_VERSION})
        endif()

        # Local extension, custom path
        message(STATUS "Load extension '${NAME}' from '${duckdb_extension_load_SOURCE_DIR}' @ ${EXT_VERSION}")

        # If no include path specified, use default
        if ("${duckdb_extension_load_INCLUDE_DIR}" STREQUAL "")
            set(INCLUDE_PATH_DEFAULT "${duckdb_extension_load_SOURCE_DIR}/src/include")
        else()
            set(INCLUDE_PATH_DEFAULT ${duckdb_extension_load_INCLUDE_DIR})
        endif()

        # If no test path specified, use default
        if ("${duckdb_extension_load_TEST_DIR}" STREQUAL "")
            set(TEST_PATH_DEFAULT "${duckdb_extension_load_SOURCE_DIR}/test/sql")
        else()
            set(TEST_PATH_DEFAULT ${duckdb_extension_load_TEST_DIR})
        endif()

        register_extension(${NAME} "${duckdb_extension_load_DONT_LINK}" "${duckdb_extension_load_DONT_BUILD}" "${duckdb_extension_load_LOAD_TESTS}" "${duckdb_extension_load_SOURCE_DIR}" "${INCLUDE_PATH_DEFAULT}" "${TEST_PATH_DEFAULT}" "${duckdb_extension_load_LINKED_LIBS}" "${EXT_VERSION}")
    elseif(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/extension_external/${NAME})
        # Local extension, default path
        message(STATUS "Load extension '${NAME}' from '${CMAKE_CURRENT_SOURCE_DIR}/extension_external' @ ${duckdb_extension_load_EXTENSION_VERSION}")
        register_extension(${NAME} ${duckdb_extension_load_DONT_LINK} "${duckdb_extension_load_DONT_BUILD}" "${duckdb_extension_load_LOAD_TESTS}"  "${CMAKE_CURRENT_SOURCE_DIR}/extension_external/${NAME}" "${CMAKE_CURRENT_SOURCE_DIR}/extension_external/${NAME}/src/include" "${CMAKE_CURRENT_SOURCE_DIR}/extension_external/${NAME}/test/sql" "${duckdb_extension_load_LINKED_LIBS}" "${duckdb_extension_load_EXTENSION_VERSION}")
    else()
        # For in-tree extensions of the default path, we set the extension version to GIT_COMMIT_HASH by default
        if ("${duckdb_extension_load_EXTENSION_VERSION}" STREQUAL "")
            set(duckdb_extension_load_EXTENSION_VERSION ${DUCKDB_NORMALIZED_VERSION})
        endif()

        # Local extension, default path
        message(STATUS "Load extension '${NAME}' from '${CMAKE_CURRENT_SOURCE_DIR}/extensions' @ ${duckdb_extension_load_EXTENSION_VERSION}")

        register_extension(${NAME} ${duckdb_extension_load_DONT_LINK} "${duckdb_extension_load_DONT_BUILD}" "${duckdb_extension_load_LOAD_TESTS}" "${CMAKE_CURRENT_SOURCE_DIR}/extension/${NAME}" "${CMAKE_CURRENT_SOURCE_DIR}/extension/${NAME}/include" "${CMAKE_CURRENT_SOURCE_DIR}/extension/${NAME}/test/sql" "${duckdb_extension_load_LINKED_LIBS}" "${duckdb_extension_load_EXTENSION_VERSION}")
    endif()

    # Propagate variables set by register_extension
    set(DUCKDB_EXTENSION_NAMES ${DUCKDB_EXTENSION_NAMES} PARENT_SCOPE)
    set(DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_SHOULD_BUILD ${DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_SHOULD_BUILD} PARENT_SCOPE)
    set(DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_SHOULD_LINK ${DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_SHOULD_LINK} PARENT_SCOPE)
    set(DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_LOAD_TESTS ${DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_LOAD_TESTS} PARENT_SCOPE)
    set(DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_PATH ${DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_PATH} PARENT_SCOPE)
    set(DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_INCLUDE_PATH ${DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_INCLUDE_PATH} PARENT_SCOPE)
    set(DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_TEST_PATH ${DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_TEST_PATH} PARENT_SCOPE)
    set(DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_LINKED_LIBS ${DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_LINKED_LIBS} PARENT_SCOPE)
    set(DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_EXT_VERSION "${DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_EXT_VERSION}" PARENT_SCOPE)
endfunction()

if(${EXPORT_DLL_SYMBOLS})
    # For Windows DLL export symbols
    add_definitions(-DDUCKDB_BUILD_LIBRARY)
endif()

# Log extensions that are built by directly passing cmake variables
foreach(EXT IN LISTS DUCKDB_EXTENSION_NAMES)
    if (NOT "${EXT}" STREQUAL "")
        string(TOUPPER ${EXT} EXTENSION_NAME_UPPERCASE)
        message(STATUS "Load extension '${EXT}' from '${DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_PATH}'")
    endif()
endforeach()

set(EXTENSION_CONFIG_BASE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/.github/config/extensions/")

if(DEFINED CORE_EXTENSIONS)
    message(DEPRECATION "CORE_EXTENSIONS is deprecated. Use BUILD_EXTENSIONS instead.")
    if(NOT DEFINED BUILD_EXTENSIONS)
        set(BUILD_EXTENSIONS ${CORE_EXTENSIONS})
    else()
        list(APPEND BUILD_EXTENSIONS ${CORE_EXTENSIONS})
    endif()
endif()

# Load extensions passed through cmake config var
foreach(EXT IN LISTS BUILD_EXTENSIONS)
    if(NOT "${EXT}" STREQUAL "")
        if (EXISTS "${EXTENSION_CONFIG_BASE_DIR}/${EXT}.cmake")
            # out-of-tree extension: load cmake file
            include("${EXTENSION_CONFIG_BASE_DIR}/${EXT}.cmake")
        else()
            # in-tree or non-existent extension: load it
            duckdb_extension_load(${EXT})
        endif()
    endif()
endforeach()

# Custom extension configs passed in DUCKDB_EXTENSION_CONFIGS parameter
foreach(DUCKDB_EXTENSION_CONFIG IN LISTS DUCKDB_EXTENSION_CONFIGS)
    if (NOT "${DUCKDB_EXTENSION_CONFIG}" STREQUAL "")
        include(${DUCKDB_EXTENSION_CONFIG})
    endif()
endforeach()

# Local extension config
if (EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/extension/extension_config_local.cmake)
    include(${CMAKE_CURRENT_SOURCE_DIR}/extension/extension_config_local.cmake)
endif()

# Load base extension config
include(${CMAKE_CURRENT_SOURCE_DIR}/extension/extension_config.cmake)

# For extensions whose tests were loaded, but not linked into duckdb, we need to ensure they are registered to have
# the sqllogictest "require" statement load the loadable extensions instead of the baked in static one
foreach(EXT_NAME IN LISTS DUCKDB_EXTENSION_NAMES)
    string(TOUPPER ${EXT_NAME} EXT_NAME_UPPERCASE)
    if (NOT "${DUCKDB_EXTENSION_${EXT_NAME_UPPERCASE}_SHOULD_LINK}" AND "${DUCKDB_EXTENSION_${EXT_NAME_UPPERCASE}_LOAD_TESTS}")
        list(APPEND TEST_WITH_LOADABLE_EXTENSION ${EXT_NAME})
    endif()
endforeach()



# Add subdirectories for registered extensions
foreach(EXT_NAME IN LISTS DUCKDB_EXTENSION_NAMES)
    string(TOUPPER ${EXT_NAME} EXT_NAME_UPPERCASE)

    if (NOT DEFINED DUCKDB_EXTENSION_${EXT_NAME_UPPERCASE}_SHOULD_BUILD)
        set(DUCKDB_EXTENSION_${EXT_NAME_UPPERCASE}_SHOULD_BUILD TRUE)
    endif()

    # Skip explicitly disabled extensions
    if (NOT ${DUCKDB_EXTENSION_${EXT_NAME_UPPERCASE}_SHOULD_BUILD} OR ${EXTENSION_CONFIG_BUILD})
        continue()
    endif()

    # Warning for trying to load vcpkg extensions without having VCPKG_BUILD SET
    if (EXISTS "${DUCKDB_EXTENSION_${EXT_NAME_UPPERCASE}_PATH}/vcpkg.json" AND NOT DEFINED VCPKG_BUILD)
        message(WARNING "Extension '${EXT_NAME}' has a vcpkg.json, but build was not run with VCPKG. If build fails, check out VCPKG build instructions in 'duckdb/extension/README.md' or try manually installing the dependencies in ${DUCKDB_EXTENSION_${EXT_NAME_UPPERCASE}_PATH}/vcpkg.json")
    endif()

    if (NOT "${DUCKDB_EXTENSION_${EXT_NAME_UPPERCASE}_EXT_VERSION}" STREQUAL "")
        add_definitions(-DEXT_VERSION_${EXT_NAME_UPPERCASE}="${DUCKDB_EXTENSION_${EXT_NAME_UPPERCASE}_EXT_VERSION}")
    endif()

    if (DEFINED DUCKDB_EXTENSION_${EXT_NAME_UPPERCASE}_PATH)
        add_subdirectory(${DUCKDB_EXTENSION_${EXT_NAME_UPPERCASE}_PATH} extension/${EXT_NAME})
    else()
        message(FATAL_ERROR "No path found for registered extension '${EXT_NAME}'")
    endif()

    if (NOT "${DUCKDB_EXTENSION_${EXT_NAME_UPPERCASE}_EXT_VERSION}" STREQUAL "")
        remove_definitions(-DEXT_VERSION_${EXT_NAME_UPPERCASE}="${DUCKDB_EXTENSION_${EXT_NAME_UPPERCASE}_EXT_VERSION}")
    endif()
endforeach()

# Output the extensions that we linked into DuckDB for some nice build logs
set(LINKED_EXTENSIONS "")
set(NONLINKED_EXTENSIONS "")
set(SKIPPED_EXTENSIONS "")
set(TEST_LOADED_EXTENSIONS "")
foreach(EXT_NAME IN LISTS DUCKDB_EXTENSION_NAMES)
    string(TOUPPER ${EXT_NAME} EXT_NAME_UPPERCASE)
    if (NOT ${DUCKDB_EXTENSION_${EXT_NAME_UPPERCASE}_SHOULD_BUILD})
        list(APPEND SKIPPED_EXTENSIONS ${EXT_NAME})
    elseif (${DUCKDB_EXTENSION_${EXT_NAME_UPPERCASE}_SHOULD_LINK})
        list(APPEND LINKED_EXTENSIONS ${EXT_NAME})
    else()
        list(APPEND NONLINKED_EXTENSIONS ${EXT_NAME})
    endif()

    if (${DUCKDB_EXTENSION_${EXT_NAME_UPPERCASE}_LOAD_TESTS})
        list(APPEND TEST_LOADED_EXTENSIONS ${EXT_NAME})
    endif()
endforeach()

if(NOT "${LINKED_EXTENSIONS}" STREQUAL "")
    string(REPLACE ";"  ", " EXT_LIST_DEBUG_MESSAGE "${LINKED_EXTENSIONS}")
    message(STATUS "Extensions linked into DuckDB: [${EXT_LIST_DEBUG_MESSAGE}]")
endif()
if(NOT "${NONLINKED_EXTENSIONS}" STREQUAL "")
    string(REPLACE ";"  ", " EXT_LIST_DEBUG_MESSAGE "${NONLINKED_EXTENSIONS}")
    message(STATUS "Extensions built but not linked: [${EXT_LIST_DEBUG_MESSAGE}]")
endif()
if(NOT "${SKIPPED_EXTENSIONS}" STREQUAL "")
    string(REPLACE ";"  ", " EXT_LIST_DEBUG_MESSAGE "${SKIPPED_EXTENSIONS}")
    message(STATUS "Extensions explicitly skipped: [${EXT_LIST_DEBUG_MESSAGE}]")
endif()
if(NOT "${TEST_LOADED_EXTENSIONS}" STREQUAL "")
    string(REPLACE ";"  ", " EXT_LIST_DEBUG_MESSAGE "${TEST_LOADED_EXTENSIONS}")
    message(STATUS "Tests loaded for extensions: [${EXT_LIST_DEBUG_MESSAGE}]")
endif()

# Special build where instead of building duckdb, we produce several artifact that require parsing the
# extension config, such as a merged vcpg.json file for extension dependencies.
if(${EXTENSION_CONFIG_BUILD})
    set(VCPKG_PATHS "")
    set(VCPKG_NAMES "")
    foreach(EXT_NAME IN LISTS DUCKDB_EXTENSION_NAMES)
        string(TOUPPER ${EXT_NAME} EXT_NAME_UPPERCASE)
        if (EXISTS "${DUCKDB_EXTENSION_${EXT_NAME_UPPERCASE}_PATH}/vcpkg.json")
            list(APPEND VCPKG_NAMES ${EXT_NAME})
            list(APPEND VCPKG_PATHS ${DUCKDB_EXTENSION_${EXT_NAME_UPPERCASE}_PATH}/vcpkg.json)
        endif()
    endforeach()

    add_custom_target(
            duckdb_merge_vcpkg_manifests ALL
            COMMAND  ${Python3_EXECUTABLE} scripts/merge_vcpkg_deps.py ${VCPKG_PATHS} ${EXT_NAMES}
            WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
            COMMENT Generates a shared vcpkg manifest from the individual extensions)
    string(REPLACE ";"  ", " VCPKG_NAMES_COMMAS "${VCPKG_NAMES}")
    Message(STATUS "Combined vcpkg manifest created from extensions: ${VCPKG_NAMES_COMMAS}")

    # Write linked extensions that will be built to extensions_linked.txt
    FILE(WRITE ${CMAKE_BINARY_DIR}/extensions.csv "")
    FILE(APPEND ${CMAKE_BINARY_DIR}/extensions.csv "name, version\r")
    foreach(EXT_NAME IN LISTS DUCKDB_EXTENSION_NAMES)
        string(TOUPPER ${EXT_NAME} EXT_NAME_UPPERCASE)
        if (${DUCKDB_EXTENSION_${EXT_NAME_UPPERCASE}_SHOULD_BUILD})
            FILE(APPEND ${CMAKE_BINARY_DIR}/extensions.csv "${EXT_NAME}, \"${DUCKDB_EXTENSION_${EXT_NAME_UPPERCASE}_EXT_VERSION}\"\r")
        endif()
    endforeach()

    return()
endif()
