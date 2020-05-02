option(BUILD_ICU_EXTENSION "Build the ICU extension." FALSE)

if(${BUILD_ICU_EXTENSION})
  add_subdirectory(icu)
endif()
