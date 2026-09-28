//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/sql_export/logical_plan_sql_exporter_internal.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_plan_sql_export_context.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/planner/sql_export_helpers.hpp"
#include "duckdb/parser/query_node/select_node.hpp"

namespace duckdb {

class Expression;
class LogicalOperator;
class LogicalMaterializedCTE;
class LogicalCTERef;
class LogicalRecursiveCTE;
class LogicalComparisonJoin;
class LogicalColumnDataGet;
class LogicalGet;
class LogicalExpressionGet;
class LogicalFilter;
class LogicalProjection;
class LogicalSecureView;
class LogicalSample;
class LogicalPivot;
class LogicalLimit;
class LogicalSetOperation;
class LogicalAggregate;
struct LogicalExtensionOperator;
class ClientContext;

using LogicalPlanSQLFieldResult = LogicalPlanVerificationResult<vector<LogicalPlanSQLExportField>>;
struct SQLSourceQueryResult {
	unique_ptr<QueryNode> query;
	string unsupported_reason;
};

//! Helpers shared by the logical operators' SQL reconstruction
struct LogicalPlanSQLExportHelpers {
	static SQLSourceQueryResult ReconstructSQLSource(ClientContext &context, const LogicalGet &get,
	                                                 unique_ptr<TableRef> input, const Identifier &relation_alias,
	                                                 bool source_ordinality);

	static LogicalPlanVerificationPath PlanChildPath(const LogicalPlanVerificationPath &path, idx_t ordinal);

	static LogicalPlanVerificationPath PlanExpressionPath(const LogicalPlanVerificationPath &path, idx_t ordinal);

	static LogicalPlanVerificationResult<unique_ptr<ParsedExpression>>
	ExportTypedNull(const LogicalType &type, const LogicalPlanVerificationPath &path);

	static LogicalPlanVerificationIssue PlanUnsupportedFeature(const LogicalPlanVerificationPath &path, string feature,
	                                                           string message);

	static LogicalPlanVerificationFunctionIdentity LogicalSourceIdentity();

	static LogicalPlanVerificationFunctionIdentity LogicalSourceIdentity(const LogicalGet &get);

	static LogicalPlanVerificationIssue UnsupportedSource(const LogicalPlanVerificationPath &path,
	                                                      LogicalPlanVerificationFunctionIdentity source, string guard);

	static Identifier FieldIdentifier(idx_t ordinal);

	static LogicalPlanSQLFieldResult CreateFields(LogicalOperator &op, const LogicalPlanVerificationPath &path);

	static BoundExpressionSQLExportContext
	CreateBindingContext(ClientContext &context, const vector<reference<const LogicalPlanSQLExportedChild>> &children,
	                     const vector<optional_ptr<const SelectNode>> &plain_scopes = {});

	static void PropagateSemanticTypes(vector<LogicalPlanSQLExportField> &fields,
	                                   const vector<reference<const LogicalPlanSQLExportedChild>> &children);

	static unique_ptr<TableRef> CreateSubquery(LogicalPlanSQLExportedChild child);

	static unique_ptr<ParsedExpression> ChildColumn(const LogicalPlanSQLExportedChild &child, idx_t field_index,
	                                                optional_ptr<const SelectNode> plain = nullptr);

	static vector<reference<const Expression>> CollectExpressions(const LogicalOperator &op);

	static bool HasEffectfulExpressions(const LogicalOperator &op);

	static bool CollectScopeAliases(const TableRef &table, identifier_set_t &aliases);

	static optional_ptr<const SelectNode> PlainScope(const QueryNode &query);

	static void SetChildScope(SelectNode &select, LogicalPlanSQLExportedChild child,
	                          optional_ptr<const SelectNode> plain);

	static bool IsIdentityProjection(const LogicalProjection &projection,
	                                 const vector<LogicalPlanSQLExportField> &fields);
};
} // namespace duckdb
