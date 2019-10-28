# don't export any symbols from the third party stuff
set(CMAKE_C_VISIBILITY_PRESET hidden)

add_subdirectory(hyperloglog)
add_subdirectory(libpg_query)
add_subdirectory(re2)
add_subdirectory(miniz)

