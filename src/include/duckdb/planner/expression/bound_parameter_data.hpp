//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression/bound_parameter_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/value.hpp"

namespace duckdb {

struct BoundParameterData {
	Value value;
	LogicalType return_type;
};

using bound_parameter_map_t = unordered_map<idx_t, shared_ptr<BoundParameterData>>;

struct BoundParameterMap {
	BoundParameterMap(vector<LogicalType> &parameter_types) : parameter_types(parameter_types) {
	}

	bound_parameter_map_t parameters;
	vector<LogicalType> &parameter_types;

	LogicalType GetReturnType(idx_t index) {
		if (index >= parameter_types.size()) {
			return LogicalTypeId::UNKNOWN;
		}
		return parameter_types[index];
	}
};

} // namespace duckdb
