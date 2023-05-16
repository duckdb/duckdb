if(${WASM_LOADABLE_EXTENSIONS})
  file(
    GLOB sources_list
    LIST_DIRECTORIES true
    "*")
  foreach(dir ${sources_list})
    if(IS_DIRECTORY ${dir})
      add_subdirectory(${dir})
    else()
      continue()
    endif()
  endforeach()
  return()
endif()