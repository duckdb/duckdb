//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/logical_plan_sql_exporter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/identifier.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/query_node.hpp"
#include "duckdb/parser/tableref.hpp"
#include "duckdb/planner/column_binding.hpp"
#include "duckdb/planner/bound_expression_sql_exporter.hpp"
#include "duckdb/planner/logical_plan_verification_result.hpp"

namespace duckdb {

class ClientContext;
class LogicalOperator;
struct LogicalExtensionOperator;

struct LogicalPlanSQLExportField {
	ColumnBinding source_binding;
	//! Type represented by the exported SQL expression.
	LogicalType type;
	//! Optimizer-selected type accepted from the bound plan, when different.
	optional<LogicalType> optimizer_type;
};

struct LogicalPlanSQLExportRelation {
	unique_ptr<QueryNode> query;
	vector<LogicalPlanSQLExportField> fields;
};

using LogicalPlanSQLExportResult = LogicalPlanVerificationResult<LogicalPlanSQLExportRelation>;

struct LogicalPlanSQLExportOptions {
	optional<vector<Identifier>> output_names;
};

class LogicalPlanSQLExporter {
public:
	//! Export a verified plan to an owned query with positional binding/type fields.
	DUCKDB_API static LogicalPlanVerificationResult<LogicalPlanSQLExportRelation>
	Export(ClientContext &context, LogicalOperator &root, const LogicalPlanSQLExportOptions &options = {});
};

} // namespace duckdb
