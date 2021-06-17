cmake_minimum_required ( VERSION 2.8.7 )


include (ExternalProject)

SET (RE2_INCLUDE_DIRS ${CMAKE_CURRENT_BINARY_DIR}/RE2/src/RE2/)
SET (RE2_EXTRA_INCLUDE_DIRS ${CMAKE_CURRENT_BINARY_DIR}/RE2/src/RE2/)
SET (RE2_URL https://github.com/google/re2)
SET (RE2_TAG cd026c5bfe9f9d713d9302ede331fa2696018b26)
SET (RE2_BUILD ${CMAKE_BINARY_DIR}/RE2/src/RE2)
SET (RE2_LIBRARIES ${RE2_BUILD}/obj/so/libre2.so)
get_filename_component(RE2_STATIC_LIBRARIES ${RE2_BUILD}/libre2.a ABSOLUTE)
SET (RE2_INCLUDES ${RE2_BUILD})

if ( WIN32 )
  SET (RE2_STATIC_LIBRARIES ${RE2_BUILD}/${CMAKE_BUILD_TYPE}/re2.lib)
else ()
  SET (RE2_STATIC_LIBRARIES ${RE2_BUILD}/libre2.a)
endif ()

ExternalProject_Add(RE2
  PREFIX RE2
  GIT_REPOSITORY ${RE2_URL}
        GIT_TAG ${RE2_TAG}
  DOWNLOAD_DIR "${DOWNLOAD_LOCATION}"
  BUILD_IN_SOURCE 1
  INSTALL_COMMAND sudo make install
   CMAKE_CACHE_ARGS
    -DCMAKE_BUILD_TYPE:STRING=Release          -DCMAKE_VERBOSE_MAKEFILE:BOOL=OFF          -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=ON
)

 ## put re2 includes in the directory where they are expected
 add_custom_target(re2_create_destination_dir COMMAND ${CMAKE_COMMAND} -E make_directory ${RE2_INCLUDE_DIR}/RE2 DEPENDS RE2)

 add_custom_target(re2_copy_headers_to_destination DEPENDS re2_create_destination_dir)

 foreach(header_file ${RE2_HEADERS})
    add_custom_command(TARGET re2_copy_headers_to_destination PRE_BUILD COMMAND ${CMAKE_COMMAND} -E copy ${header_file} ${RE2_INCLUDE_DIR}/re2)
  endforeach ()

  ADD_LIBRARY(RE2_LIB STATIC IMPORTED DEPENDS RE2)
  SET_TARGET_PROPERTIES(RE2_LIB PROPERTIES IMPORTED_LOCATION ${RE2_STATIC_LIBRARIES})

