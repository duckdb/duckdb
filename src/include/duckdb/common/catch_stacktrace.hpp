#pragma once

#include "duckdb/common/exception.hpp"

#ifdef DUCKDB_DEBUG_STACKTRACE
#define CATCH_STACKTRACE(X) duckdb::Exception::FormatStackTrace(X).c_str()
#endif

#include "catch.hpp"
