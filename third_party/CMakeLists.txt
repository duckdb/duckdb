# don't export any symbols from the third party stuff
set(CMAKE_C_VISIBILITY_PRESET hidden)

if(NOT AMALGAMATION_BUILD)
add_subdirectory(hyperloglog)
add_subdirectory(fmt)
add_subdirectory(libpg_query)
add_subdirectory(re2)
add_subdirectory(miniz)
add_subdirectory(utf8proc)

endif()

if(NOT WIN32 AND NOT SUN)
  add_subdirectory(imdb)
  add_subdirectory(dbgen)
  add_subdirectory(dsdgen)
  add_subdirectory(sqlsmith)
  add_subdirectory(sqlite)
  add_subdirectory(tpce-tool)

endif()

add_subdirectory(miniparquet)
