//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/common.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#ifdef _WIN32
#ifdef DUCKDB_MAIN_LIBRARY
#ifndef _WINSOCKAPI_
#define _WINSOCKAPI_
#endif

#include <windows.h>
#endif
#endif

#include "duckdb/common/constants.hpp"
#include "duckdb/common/helper.hpp"
