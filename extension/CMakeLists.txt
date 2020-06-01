option(BUILD_ICU_EXTENSION "Build the ICU extension." FALSE)

if(${BUILD_ICU_EXTENSION})
  add_subdirectory(icu)
endif()

option(BUILD_PARQUET_EXTENSION "Build the Parquet extension." FALSE)

if(${BUILD_PARQUET_EXTENSION})
  add_subdirectory(parquet)
endif()
