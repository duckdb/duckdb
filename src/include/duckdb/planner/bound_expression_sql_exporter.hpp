//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/bound_expression_sql_exporter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/identifier.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/planner/column_binding.hpp"
#include "duckdb/planner/logical_plan_compiler_result.hpp"

#include <functional>

namespace duckdb {

class Expression;

struct ResolvedSQLColumnReference {
	vector<Identifier> names;
	LogicalType type;
};

using BoundExpressionSQLBindingResolver =
    std::function<optional<ResolvedSQLColumnReference>(const ColumnBinding &binding)>;

struct BoundExpressionSQLExportContext {
	BoundExpressionSQLBindingResolver resolve_binding;
};

class BoundExpressionSQLExporter {
public:
	DUCKDB_API static LogicalPlanCompilerResult<unique_ptr<ParsedExpression>>
	Export(const Expression &expression, const BoundExpressionSQLExportContext &context);

	DUCKDB_API static LogicalPlanCompilerResult<unique_ptr<ParsedExpression>>
	ExportAtPath(const Expression &expression, const BoundExpressionSQLExportContext &context,
	             const LogicalPlanCompilerPath &path);
};

} // namespace duckdb
