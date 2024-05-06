if(NOT SUN AND BUILD_SHELL)
  add_subdirectory(sqlite3_api_wrapper)
  add_subdirectory(shell)
endif()

if(BUILD_ODBC_DRIVER)
  add_subdirectory(odbc)
endif()

# Just to make CLion aware of the python package for development purposes
if(BUILD_PYTHON_PKG)
  add_subdirectory(pythonpkg)
endif()
