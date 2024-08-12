//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/csv_scanner/header_value.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/string_type.hpp"

namespace duckdb {
struct HeaderValue {
	HeaderValue() : is_null(true) {
	}
	explicit HeaderValue(const string_t value_p) {
		value = value_p.GetString();
	}
	bool IsNull() {
		return is_null;
	}
	bool is_null = false;
	string value;
};
} // namespace duckdb
