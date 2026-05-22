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
	// parameter_type_hints carries client-supplied per-parameter
	// LogicalTypes (e.g. PG protocol Parse OIDs). Unlike parameter_data, hints
	// only declare a type -- they do not trigger constant-folding -- so the
	// parameter survives as a real bound parameter for re-bind at execute.
	explicit BoundParameterMap(case_insensitive_map_t<BoundParameterData> &parameter_data,
	                           optional_ptr<const case_insensitive_map_t<LogicalType>> parameter_type_hints = nullptr);

public:
	LogicalType GetReturnType(const string &identifier);

	bound_parameter_map_t *GetParametersPtr();

	const bound_parameter_map_t &GetParameters();

	const case_insensitive_map_t<BoundParameterData> &GetParameterData();

	unique_ptr<BoundParameterExpression> BindParameterExpression(ParameterExpression &expr);

	//! Flag to indicate that we need to rebind this prepared statement before execution
	bool rebind = false;

private:
	shared_ptr<BoundParameterData> CreateOrGetData(const string &identifier);
	void CreateNewParameter(const string &id, const shared_ptr<BoundParameterData> &param_data);

private:
	bound_parameter_map_t parameters;
	// Pre-provided parameter data if populated
	case_insensitive_map_t<BoundParameterData> &parameter_data;
	// Type-only hints, consulted by GetReturnType when parameter_data lacks the entry.
	optional_ptr<const case_insensitive_map_t<LogicalType>> parameter_type_hints;
};

} // namespace duckdb
