//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/interval.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"

namespace duckdb {

//! The Interval class is a static class that holds helper functions for the Interval
//! type.
class Interval {
public:
	//! Convert a string to an interval object
	static bool FromString(string str, interval_t &result);
	//! Convert a string to an interval object
	static bool FromCString(const char *str, idx_t len, interval_t &result);
	//! Convert an interval object to a string
	static string ToString(interval_t date);
};
} // namespace duckdb
