cmake_minimum_required ( VERSION 2.8.7 )

if (NOT ZLIB_NAME)

include (ExternalProject)

SET (ZLIB_INCLUDE_DIRS ${CMAKE_CURRENT_BINARY_DIR}/ZLIB/src/ZLIB/)
SET (ZLIB_EXTRA_INCLUDE_DIRS ${CMAKE_CURRENT_BINARY_DIR}/ZLIB/src/ZLIB/)
SET (ZLIB_URL https://github.com/madler/zlib)
SET (ZLIB_BUILD ${CMAKE_BINARY_DIR}/ZLIB/src/ZLIB/build)
SET (ZLIB_LIBRARIES ${ZLIB_BUILD}/obj/so/libZLIB.so)
get_filename_component(ZLIB_STATIC_LIBRARIES ${ZLIB_BUILD}/libZLIB.a ABSOLUTE)
SET (ZLIB_INCLUDES ${ZLIB_BUILD})

if ( WIN32 )
  SET (ZLIB_STATIC_LIBRARIES ${ZLIB_BUILD}/${CMAKE_BUILD_TYPE}/libz.lib)
else ()
  SET (ZLIB_STATIC_LIBRARIES ${ZLIB_BUILD}/libz.a)
endif ()

ExternalProject_Add(ZLIB
  PREFIX ZLIB
  GIT_REPOSITORY ${ZLIB_URL}
  # GIT_TAG ${ZLIB_TAG}
  DOWNLOAD_DIR "${DOWNLOAD_LOCATION}"
  BUILD_IN_SOURCE 0
  INSTALL_COMMAND sudo make install
   CMAKE_CACHE_ARGS
    -DCMAKE_BUILD_TYPE:STRING=Release          -DCMAKE_VERBOSE_MAKEFILE:BOOL=OFF          -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=ON
)

 ## put ZLIB includes in the directory where they are expected
 add_custom_target(ZLIB_create_destination_dir COMMAND ${CMAKE_COMMAND} -E make_directory ${ZLIB_INCLUDE_DIR}/ZLIB DEPENDS ZLIB)

 add_custom_target(ZLIB_copy_headers_to_destination DEPENDS ZLIB_create_destination_dir)

 foreach(header_file ${ZLIB_HEADERS})
    add_custom_command(TARGET ZLIB_copy_headers_to_destination PRE_BUILD COMMAND ${CMAKE_COMMAND} -E copy ${header_file} ${ZLIB_INCLUDE_DIR}/ZLIB)
  endforeach ()

  ADD_LIBRARY(ZLIB_LIB STATIC IMPORTED DEPENDS ZLIB)
  SET_TARGET_PROPERTIES(ZLIB_LIB PROPERTIES IMPORTED_LOCATION ${ZLIB_STATIC_LIBRARIES})

endif (NOT ZLIB_NAME)