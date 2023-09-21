//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/bound_parameter_map.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/planner/expression/bound_parameter_data.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

namespace duckdb {

class ParameterExpression;
class BoundParameterExpression;

using bound_parameter_map_t = case_insensitive_map_t<shared_ptr<BoundParameterData>>;

struct BoundParameterMap {
public:
	explicit BoundParameterMap(case_insensitive_map_t<BoundParameterData> &parameter_data);

public:
	LogicalType GetReturnType(const string &identifier);

	bound_parameter_map_t *GetParametersPtr();

	const bound_parameter_map_t &GetParameters();

	const case_insensitive_map_t<BoundParameterData> &GetParameterData();

	unique_ptr<BoundParameterExpression> BindParameterExpression(ParameterExpression &expr);

private:
	shared_ptr<BoundParameterData> CreateOrGetData(const string &identifier);
	void CreateNewParameter(const string &id, shared_ptr<BoundParameterData> param_data);

private:
	bound_parameter_map_t parameters;
	// A reference to the map that owns the BoundParameterData
	// the object that should get populated with the Value to bind the prepared parameter
	case_insensitive_map_t<BoundParameterData> &parameter_data;
};

} // namespace duckdb
