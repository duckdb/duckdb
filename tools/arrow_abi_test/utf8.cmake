cmake_minimum_required ( VERSION 2.8.7 )

if (NOT UTF8_NAME)

include (ExternalProject)

SET (UTF8_INCLUDE_DIRS ${CMAKE_CURRENT_BINARY_DIR}/UTF8/src/UTF8/)
SET (UTF8_EXTRA_INCLUDE_DIRS ${CMAKE_CURRENT_BINARY_DIR}/UTF8/src/UTF8/)
SET (UTF8_URL https://github.com/JuliaStrings/utf8proc)
SET (UTF8_BUILD ${CMAKE_BINARY_DIR}/UTF8/src/UTF8/build)
SET (UTF8_LIBRARIES ${UTF8_BUILD}/obj/so/libUTF8.so)
get_filename_component(UTF8_STATIC_LIBRARIES ${UTF8_BUILD}/libUTF8.a ABSOLUTE)
SET (UTF8_INCLUDES ${UTF8_BUILD})

if ( WIN32 )
  SET (UTF8_STATIC_LIBRARIES ${UTF8_BUILD}/${CMAKE_BUILD_TYPE}/UTF8.lib)
else ()
  SET (UTF8_STATIC_LIBRARIES ${UTF8_BUILD}/libUTF8.a)
endif ()

ExternalProject_Add(UTF8
  PREFIX UTF8
  GIT_REPOSITORY ${UTF8_URL}
  # GIT_TAG ${UTF8_TAG}
  DOWNLOAD_DIR "${DOWNLOAD_LOCATION}"
  BUILD_IN_SOURCE 0
  INSTALL_COMMAND sudo make install
   CMAKE_CACHE_ARGS
    -DCMAKE_BUILD_TYPE:STRING=Release          -DCMAKE_VERBOSE_MAKEFILE:BOOL=OFF          -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=ON
)

 ## put UTF8 includes in the directory where they are expected
 add_custom_target(UTF8_create_destination_dir COMMAND ${CMAKE_COMMAND} -E make_directory ${UTF8_INCLUDE_DIR}/UTF8 DEPENDS UTF8)

 add_custom_target(UTF8_copy_headers_to_destination DEPENDS UTF8_create_destination_dir)

 foreach(header_file ${UTF8_HEADERS})
    add_custom_command(TARGET UTF8_copy_headers_to_destination PRE_BUILD COMMAND ${CMAKE_COMMAND} -E copy ${header_file} ${UTF8_INCLUDE_DIR}/UTF8)
  endforeach ()

  ADD_LIBRARY(UTF8_LIB STATIC IMPORTED DEPENDS UTF8)
  SET_TARGET_PROPERTIES(UTF8_LIB PROPERTIES IMPORTED_LOCATION ${UTF8_STATIC_LIBRARIES})

endif (NOT UTF8_NAME)