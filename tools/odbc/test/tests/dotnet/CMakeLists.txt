cmake_minimum_required (VERSION 3.20.0)

message("cmake version ${CMAKE_VERSION}")

project (SystemDataODBC_tests VERSION 0.0.0)

include(CSharpUtilities)

ADD_EXECUTABLE(SystemDataODBC_tests 
    odbcExampleCpp.cpp
    AssemblyInfo.cpp
    pch.cpp)

set(CMAKE_CSharp_FLAGS "/langversion:latest /platform:x64")

csharp_set_designer_cs_properties(
      AssemblyInfo.cpp)
      
set_property(TARGET ${PROJECT_NAME} PROPERTY COMMON_LANGUAGE_RUNTIME "")

# set_property(TARGET ${PROJECT_NAME} PROPERTY COMPILE_FLAGS "/clr /EHa")

set_property(TARGET ${PROJECT_NAME} PROPERTY VS_DOTNET_TARGET_FRAMEWORK_VERSION "v4.8.1")

set_property(TARGET ${PROJECT_NAME} PROPERTY VS_DOTNET_REFERENCES
      "Microsoft.CSharp"
      "System"
      "System.Core"
      "System.Data"
      "System.Data.DataSetExtensions"
      "System.Net.Http"
      "System.Xml"
      "System.Xml.Linq")



