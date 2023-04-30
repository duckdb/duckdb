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

if(${BUILD_ICU_EXTENSION})
  add_subdirectory(icu)
endif()

if(${BUILD_PARQUET_EXTENSION})
  add_subdirectory(parquet)
endif()

if(${BUILD_TPCH_EXTENSION})
  add_subdirectory(tpch)
endif()

if(${BUILD_TPCDS_EXTENSION})
  add_subdirectory(tpcds)
endif()

if(${BUILD_FTS_EXTENSION})
  add_subdirectory(fts)
endif()

if(${BUILD_HTTPFS_EXTENSION})
  add_subdirectory(httpfs)
endif()

if(${BUILD_VISUALIZER_EXTENSION})
  add_subdirectory(visualizer)
endif()

if(${BUILD_JSON_EXTENSION})
  add_subdirectory(json)
endif()

if(${BUILD_JEMALLOC_EXTENSION})
  add_subdirectory(jemalloc)
endif()

if(${BUILD_EXCEL_EXTENSION})
  add_subdirectory(excel)
endif()

if(${BUILD_SQLSMITH_EXTENSION})
  add_subdirectory(sqlsmith)
endif()

if(${BUILD_INET_EXTENSION})
  add_subdirectory(inet)
endif()

if(${BUILD_AUTOCOMPLETE_EXTENSION})
  add_subdirectory(autocomplete)
endif()
