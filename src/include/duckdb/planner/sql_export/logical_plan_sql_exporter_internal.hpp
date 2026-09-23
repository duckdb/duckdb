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

namespace logical_plan_sql_export {

using LogicalPlanSQLExportResult = LogicalPlanVerificationResult<LogicalPlanSQLExportRelation>;

using LogicalPlanSQLFieldResult = LogicalPlanVerificationResult<vector<LogicalPlanSQLExportField>>;

struct SQLSourceQueryResult {
	unique_ptr<QueryNode> query;
	string unsupported_reason;
};

SQLSourceQueryResult ReconstructSQLSource(ClientContext &context, const LogicalGet &get, unique_ptr<TableRef> input,
                                          const Identifier &relation_alias, bool source_ordinality);

LogicalPlanVerificationPath PlanChildPath(const LogicalPlanVerificationPath &path, idx_t ordinal);

LogicalPlanVerificationPath PlanExpressionPath(const LogicalPlanVerificationPath &path, idx_t ordinal);

LogicalPlanSQLExportResult PlanFailure(LogicalPlanVerificationIssue issue);

LogicalPlanVerificationResult<unique_ptr<ParsedExpression>> ExportTypedNull(const LogicalType &type,
                                                                            const LogicalPlanVerificationPath &path);

LogicalPlanSQLFieldResult FieldFailure(LogicalPlanVerificationIssue issue);

LogicalPlanVerificationIssue PlanUnsupportedFeature(const LogicalPlanVerificationPath &path, string feature,
                                                    string message);

LogicalPlanVerificationFunctionIdentity LogicalSourceIdentity();

LogicalPlanVerificationFunctionIdentity LogicalSourceIdentity(const LogicalGet &get);

LogicalPlanVerificationIssue UnsupportedSource(const LogicalPlanVerificationPath &path,
                                               LogicalPlanVerificationFunctionIdentity source, string guard);

Identifier FieldIdentifier(idx_t ordinal);

LogicalPlanSQLFieldResult CreateFields(LogicalOperator &op, const LogicalPlanVerificationPath &path);

BoundExpressionSQLExportContext
CreateBindingContext(ClientContext &context, const vector<reference<const LogicalPlanSQLExportedChild>> &children,
                     const vector<optional_ptr<const SelectNode>> &plain_scopes = {});

void PropagateSemanticTypes(vector<LogicalPlanSQLExportField> &fields,
                            const vector<reference<const LogicalPlanSQLExportedChild>> &children);

unique_ptr<TableRef> CreateSubquery(LogicalPlanSQLExportedChild child);

unique_ptr<ParsedExpression> ChildColumn(const LogicalPlanSQLExportedChild &child, idx_t field_index,
                                         optional_ptr<const SelectNode> plain = nullptr);

vector<reference<const Expression>> CollectExpressions(const LogicalOperator &op);

bool HasEffectfulExpressions(const LogicalOperator &op);

bool CollectScopeAliases(const TableRef &table, identifier_set_t &aliases);

optional_ptr<const SelectNode> PlainScope(const QueryNode &query);

void SetChildScope(SelectNode &select, LogicalPlanSQLExportedChild child, optional_ptr<const SelectNode> plain);

bool IsIdentityProjection(const LogicalProjection &projection, const vector<LogicalPlanSQLExportField> &fields);

} // namespace logical_plan_sql_export
} // namespace duckdb
