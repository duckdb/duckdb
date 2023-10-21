project(
  SystemDataODBC_tests
  VERSION 0.0.0
  LANGUAGES CXX)

set(SOURCES odbcExampleCpp.cpp AssemblyInfo.cpp pch.cpp)

add_executable(${PROJECT_NAME} ${SOURCES})

# "" (set but empty) == Mixed unmanaged/managed C++ using .NET Framework.
set_target_properties(${PROJECT_NAME} PROPERTIES COMMON_LANGUAGE_RUNTIME "")

set_target_properties(${PROJECT_NAME}
                      PROPERTIES VS_DOTNET_TARGET_FRAMEWORK_VERSION "v4.8.1")

list(APPEND VS_DOTNET_REFERENCES "Microsoft.CSharp")
list(APPEND VS_DOTNET_REFERENCES "System")
list(APPEND VS_DOTNET_REFERENCES "System.Core")
list(APPEND VS_DOTNET_REFERENCES "System.Data")
list(APPEND VS_DOTNET_REFERENCES "System.Data.DataSetExtensions")
list(APPEND VS_DOTNET_REFERENCES "System.Net.Http")
list(APPEND VS_DOTNET_REFERENCES "System.Xml")
list(APPEND VS_DOTNET_REFERENCES "System.Xml.Linq")

set_target_properties(${PROJECT_NAME} PROPERTIES VS_DOTNET_REFERENCES
                                                 "${VS_DOTNET_REFERENCES}")
