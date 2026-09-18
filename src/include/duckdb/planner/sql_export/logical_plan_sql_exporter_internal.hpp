#pragma once

#include "duckdb/planner/logical_plan_sql_exporter.hpp"
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
class LogicalExtensionOperator;
class ClientContext;

namespace logical_plan_sql_export {

using LogicalPlanSQLExportResult = LogicalPlanVerificationResult<LogicalPlanSQLExportRelation>;

using LogicalPlanSQLFieldResult = LogicalPlanVerificationResult<vector<LogicalPlanSQLExportField>>;

struct LogicalPlanSQLExportedChild {
	LogicalPlanSQLExportRelation relation;
	Identifier relation_alias;
};

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

class LogicalPlanSQLExportState {
public:
	LogicalPlanSQLExportState(ClientContext &context_p, const LogicalPlanSQLExportOptions &options_p);
	LogicalPlanSQLExportResult Export(LogicalOperator &op, const LogicalPlanVerificationPath &path);

private:
	LogicalPlanSQLExportResult ExportOperator(LogicalOperator &op, const LogicalPlanVerificationPath &path);
	Identifier NextRelationAlias(const Identifier &preferred = Identifier());
	LogicalPlanVerificationResult<LogicalPlanSQLExportedChild> ExportChild(LogicalOperator &child,
	                                                                       const LogicalPlanVerificationPath &path);
	LogicalPlanVerificationResult<unique_ptr<ParsedExpression>>
	ExportExpression(const LogicalOperator &op, const vector<reference<const Expression>> &expressions,
	                 idx_t expression_ordinal, const BoundExpressionSQLExportContext &expression_context,
	                 const LogicalPlanVerificationPath &path);
	LogicalPlanSQLExportResult ExportMaterializedCTE(LogicalMaterializedCTE &cte,
	                                                 const LogicalPlanVerificationPath &path);
	LogicalPlanSQLExportResult ExportCTERef(LogicalCTERef &ref, const LogicalPlanVerificationPath &path);
	unique_ptr<SelectNode> CreateNamedSource(const Identifier &name, const vector<LogicalPlanSQLExportField> &fields,
	                                         bool recurring = false);
	LogicalPlanSQLExportResult BuildRecursiveCTE(LogicalRecursiveCTE &cte, const LogicalPlanVerificationPath &path,
	                                             const Identifier &name);
	LogicalPlanVerificationResult<LogicalPlanSQLExportedChild>
	ExportNamedProducer(LogicalOperator &op, const LogicalPlanVerificationPath &path, const Identifier &name);
	LogicalPlanSQLExportResult ExportRecursiveCTE(LogicalRecursiveCTE &cte, const LogicalPlanVerificationPath &path);
	LogicalPlanSQLExportResult ExportContextExpressions(LogicalOperator &op, const LogicalPlanVerificationPath &path);
	string MarkConditionUnsupportedReason(const LogicalComparisonJoin &join);
	bool RequiresMarkGroupMetadata(const LogicalComparisonJoin &join);
	LogicalPlanSQLExportResult ExportJoin(LogicalOperator &op, const LogicalPlanVerificationPath &path);
	LogicalPlanSQLExportResult ExportChunkGet(LogicalColumnDataGet &get, const LogicalPlanVerificationPath &path);
	LogicalPlanSQLExportResult ExportGet(LogicalGet &get, const LogicalPlanVerificationPath &path,
	                                     optional<LogicalPlanSQLExportField> ordinality = {});
	LogicalPlanSQLExportResult ExportExpressionGet(LogicalExpressionGet &get, const LogicalPlanVerificationPath &path);
	optional<LogicalPlanVerificationIssue> CheckExpressionGetInput(LogicalExpressionGet &get,
	                                                               const LogicalPlanVerificationPath &path);
	LogicalPlanSQLExportResult ExportExpressionGetInput(LogicalExpressionGet &get,
	                                                    const LogicalPlanVerificationPath &path,
	                                                    vector<LogicalPlanSQLExportField> fields);
	LogicalPlanSQLExportResult ExportRow(unique_ptr<SelectNode> select, unique_ptr<ParsedExpression> value,
	                                     vector<LogicalPlanSQLExportField> fields);
	LogicalPlanSQLExportResult ExportFilter(LogicalFilter &filter, const LogicalPlanVerificationPath &path);
	LogicalPlanSQLExportResult ExportProjection(LogicalProjection &projection, const LogicalPlanVerificationPath &path);
	LogicalPlanSQLExportResult ExportSecureView(LogicalSecureView &view, const LogicalPlanVerificationPath &path);
	LogicalPlanSQLExportResult ExportConstantSource(LogicalOperator &op, const LogicalPlanVerificationPath &path);
	unique_ptr<SelectNode> ForwardFields(const LogicalPlanSQLExportedChild &child,
	                                     const vector<LogicalPlanSQLExportField> &fields,
	                                     optional_ptr<const SelectNode> plain = nullptr);
	LogicalPlanSQLExportResult ExportModifier(LogicalOperator &op, const LogicalPlanVerificationPath &path);
	LogicalPlanSQLExportResult ExportSample(LogicalSample &sample, const LogicalPlanVerificationPath &path);
	LogicalPlanVerificationResult<unique_ptr<ParsedExpression>>
	ExportPivotDefault(const BoundAggregateExpression &aggregate, const LogicalPlanVerificationPath &path);
	LogicalPlanSQLExportResult ExportPivot(LogicalPivot &pivot, const LogicalPlanVerificationPath &path);
	bool ProducesOneRow(const LogicalOperator &op, const vector<TableIndex> &single_row_ctes = {});
	using LimitExpressionResult = LogicalPlanVerificationResult<unique_ptr<ParsedExpression>>;
	LimitExpressionResult LimitBindingFailure(const LogicalPlanVerificationPath &path);
	LimitExpressionResult ResolveLimitColumn(const ColumnBinding &binding, LogicalOperator &input,
	                                         const LogicalPlanVerificationPath &path);
	LimitExpressionResult ExportLimitExpression(const Expression &expression, LogicalOperator &input,
	                                            const LogicalPlanVerificationPath &path,
	                                            const LogicalPlanVerificationPath &expression_path);
	LogicalPlanSQLExportResult ExportLimit(LogicalLimit &limit, const LogicalPlanVerificationPath &path);
	LogicalPlanSQLExportResult ExportSetOperation(LogicalSetOperation &op, const LogicalPlanVerificationPath &path);
	LogicalPlanSQLExportResult ExportAggregate(LogicalAggregate &aggregate, const LogicalPlanVerificationPath &path);
	LogicalPlanSQLExportResult ExportExtension(LogicalExtensionOperator &extension,
	                                           const LogicalPlanVerificationPath &path);
	optional<LogicalPlanSQLExportResult> HandleExtensionResult(const LogicalPlanVerificationPath &path,
	                                                           const string &extension_identifier,
	                                                           LogicalPlanSQLExportExtensionResult result,
	                                                           const vector<LogicalPlanSQLExportField> &fields);
	struct LimitSource {
		optional_ptr<LogicalOperator> op;
		Identifier name;
		LogicalPlanSQLExportRelation relation;
	};
	vector<LimitSource> limit_sources;
	struct NamedRelation {
		TableIndex index;
		Identifier name;
		bool is_recurring;
		idx_t references;
	};
	ClientContext &context;
	const LogicalPlanSQLExportOptions &options;
	idx_t next_relation_ordinal = 0;
	identifier_set_t relation_aliases;
	vector<reference<LogicalOperator>> ancestors;
	vector<NamedRelation> named_relations;
};

} // namespace logical_plan_sql_export
} // namespace duckdb
