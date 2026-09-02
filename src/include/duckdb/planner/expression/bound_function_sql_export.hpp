//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression/bound_function_sql_export.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/function_sql_export.hpp"
#include "duckdb/parser/qualified_name.hpp"

namespace duckdb {

enum class BoundFunctionSQLExportType : uint8_t { CATALOG, CAST, COMPARISON, BETWEEN };

struct BoundScalarFunctionSQLExportRecipe {
	BoundFunctionSQLExportType type;
	QualifiedName name;
	vector<LogicalType> arguments;
	LogicalType return_type;
	scalar_function_sql_export_t callback = nullptr;
	bool requires_callback = false;
};

struct BoundAggregateFunctionSQLExportRecipe {
	BoundFunctionSQLExportType type;
	QualifiedName name;
	vector<LogicalType> arguments;
	LogicalType return_type;
	aggregate_function_sql_export_t callback = nullptr;
	bool requires_callback = false;
};

} // namespace duckdb
