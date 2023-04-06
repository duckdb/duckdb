#pragma once

#include "duckdb/common/exception.hpp"

#ifndef CATCH_STACKTRACE
#define CATCH_STACKTRACE(X) duckdb::Exception::FormatStackTrace(X).c_str()
#endif

#include "catch.hpp"
