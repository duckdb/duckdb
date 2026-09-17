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

#include <functional>

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

enum class LogicalPlanSQLExportExtensionResultType { NOT_HANDLED, EXPORTED, UNSUPPORTED };

struct LogicalPlanSQLExportExtensionResult {
	LogicalPlanSQLExportExtensionResultType type = LogicalPlanSQLExportExtensionResultType::NOT_HANDLED;
	//! EXPORTED requires a query; other results must leave it empty.
	unique_ptr<QueryNode> query;
	//! UNSUPPORTED requires a nonempty reason; other results must leave it empty.
	string reason;

	DUCKDB_API static LogicalPlanSQLExportExtensionResult NotHandled();
	DUCKDB_API static LogicalPlanSQLExportExtensionResult Exported(unique_ptr<QueryNode> query);
	DUCKDB_API static LogicalPlanSQLExportExtensionResult Unsupported(string reason);
};

struct LogicalPlanSQLExportChild {
	explicit LogicalPlanSQLExportChild(unique_ptr<TableRef> table_p) : table(std::move(table_p)) {
	}

	//! Ready to move into a callback's query, with positional column names already assigned.
	unique_ptr<TableRef> table;
};

using LogicalPlanSQLExpressionExporter =
    std::function<LogicalPlanVerificationResult<unique_ptr<ParsedExpression>>(idx_t expression_ordinal)>;

struct LogicalPlanSQLExportExtensionInput {
	LogicalPlanSQLExportExtensionInput(const LogicalExtensionOperator &op_p,
	                                   vector<LogicalPlanSQLExportChild> &children_p, idx_t expression_count_p,
	                                   LogicalPlanSQLExpressionExporter export_expression_p,
	                                   BoundExpressionSQLExportContext binding_context_p)
	    : op(op_p), children(children_p), expression_count(expression_count_p),
	      export_expression(std::move(export_expression_p)), binding_context(std::move(binding_context_p)) {
	}

	const LogicalExtensionOperator &op;
	//! NOT_HANDLED must leave children untouched. Consuming a child requires a terminal result.
	vector<LogicalPlanSQLExportChild> &children;
	const idx_t expression_count;
	//! Input references and expression helpers are valid only during the callback.
	const LogicalPlanSQLExpressionExporter export_expression;
	const BoundExpressionSQLExportContext binding_context;
};

using logical_plan_sql_export_t =
    std::function<LogicalPlanSQLExportExtensionResult(const LogicalPlanSQLExportExtensionInput &input)>;

struct LogicalPlanSQLExportOptions {
	logical_plan_sql_export_t extension_resolver;
	optional<vector<Identifier>> output_names;
};

class LogicalPlanSQLExporter {
public:
	//! Export a verified plan to an owned query with positional binding/type fields.
	DUCKDB_API static LogicalPlanVerificationResult<LogicalPlanSQLExportRelation>
	Export(ClientContext &context, LogicalOperator &root, const LogicalPlanSQLExportOptions &options = {});
};

} // namespace duckdb
