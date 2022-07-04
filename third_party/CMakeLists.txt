# don't export any symbols from the third party stuff
set(CMAKE_C_VISIBILITY_PRESET hidden)

if(NOT AMALGAMATION_BUILD)
  add_subdirectory(fmt)
  add_subdirectory(libpg_query)
  add_subdirectory(re2)
  add_subdirectory(miniz)
  add_subdirectory(utf8proc)
  add_subdirectory(hyperloglog)
  add_subdirectory(fastpforlib)
endif()

if(NOT WIN32
   AND NOT SUN
   AND ${BUILD_UNITTESTS})
  add_subdirectory(imdb)
  add_subdirectory(sqlite)
  if(${BUILD_TPCE})
    add_subdirectory(tpce-tool)
  endif()
endif()
