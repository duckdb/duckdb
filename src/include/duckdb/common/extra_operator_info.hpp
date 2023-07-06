//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/extra_operator_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>
#include <cstdint>
#include <cstring>
#include "duckdb/common/operator/comparison_operators.hpp"

namespace duckdb {

class ExtraOperatorInfo {
public:
	ExtraOperatorInfo() : file_filters("") {
	}
	ExtraOperatorInfo(ExtraOperatorInfo &extra_info) : file_filters(extra_info.file_filters) {
	}
	string file_filters;
};

} // namespace duckdb
