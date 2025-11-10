# don't export any symbols from the third party stuff
set(CMAKE_C_VISIBILITY_PRESET hidden)

if(NOT AMALGAMATION_BUILD)
  add_subdirectory(fmt)
  add_subdirectory(libpg_query)
  add_subdirectory(re2)
  add_subdirectory(miniz)
  add_subdirectory(utf8proc)
  add_subdirectory(hyperloglog)
  add_subdirectory(skiplist)
  add_subdirectory(fastpforlib)
  add_subdirectory(mbedtls)
  add_subdirectory(fsst)
  add_subdirectory(yyjson)
  add_subdirectory(zstd)
endif()

if(NOT WIN32
   AND ${BUILD_UNITTESTS} AND ${BUILD_TPCE})
    add_subdirectory(tpce-tool)
endif()

if (${BUILD_BENCHMARKS})
  add_subdirectory(imdb)
endif()