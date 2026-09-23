#pragma once

#include "duckdb/planner/logical_plan_sql_exporter.hpp"
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
struct LogicalPlanSQLExportedChild {
	LogicalPlanSQLExportRelation relation;
	Identifier relation_alias;
};

class LogicalPlanSQLExportContext {
public:
	explicit LogicalPlanSQLExportContext(ClientContext &context_p);
	LogicalPlanSQLExportResult Export(LogicalOperator &op, const LogicalPlanVerificationPath &path);

public:
	ClientContext &GetClientContext() const {
		return context;
	}

	Identifier NextRelationAlias(const Identifier &preferred = Identifier());
	LogicalPlanVerificationResult<LogicalPlanSQLExportedChild> ExportChild(LogicalOperator &child,
	                                                                       const LogicalPlanVerificationPath &path);
	LogicalPlanVerificationResult<unique_ptr<ParsedExpression>>
	ExportExpression(const LogicalOperator &op, const vector<reference<const Expression>> &expressions,
	                 idx_t expression_ordinal, const BoundExpressionSQLExportContext &expression_context,
	                 const LogicalPlanVerificationPath &path);
	unique_ptr<SelectNode> CreateNamedSource(const Identifier &name, const vector<LogicalPlanSQLExportField> &fields,
	                                         bool recurring = false);

	LogicalPlanVerificationResult<LogicalPlanSQLExportedChild>
	ExportNamedProducer(LogicalOperator &op, const LogicalPlanVerificationPath &path, const Identifier &name);

	LogicalPlanSQLExportResult ExportRow(unique_ptr<SelectNode> select, unique_ptr<ParsedExpression> value,
	                                     vector<LogicalPlanSQLExportField> fields);
	unique_ptr<SelectNode> ForwardFields(const LogicalPlanSQLExportedChild &child,
	                                     const vector<LogicalPlanSQLExportField> &fields,
	                                     optional_ptr<const SelectNode> plain = nullptr);

	bool ProducesOneRow(const LogicalOperator &op, const vector<TableIndex> &single_row_ctes = {});
	using LimitExpressionResult = LogicalPlanVerificationResult<unique_ptr<ParsedExpression>>;
	LimitExpressionResult LimitBindingFailure(const LogicalPlanVerificationPath &path);
	LimitExpressionResult ResolveLimitColumn(const ColumnBinding &binding, LogicalOperator &input,
	                                         const LogicalPlanVerificationPath &path);
	LimitExpressionResult ExportLimitExpression(const Expression &expression, LogicalOperator &input,
	                                            const LogicalPlanVerificationPath &path,
	                                            const LogicalPlanVerificationPath &expression_path);

private:
	friend class duckdb::LogicalMaterializedCTE;
	friend class duckdb::LogicalCTERef;
	friend class duckdb::LogicalRecursiveCTE;
	friend class duckdb::LogicalColumnDataGet;
	friend class duckdb::LogicalGet;
	friend class duckdb::LogicalExpressionGet;
	friend class duckdb::LogicalFilter;
	friend class duckdb::LogicalProjection;
	friend class duckdb::LogicalSecureView;
	friend class duckdb::LogicalSample;
	friend class duckdb::LogicalPivot;
	friend class duckdb::LogicalLimit;
	friend class duckdb::LogicalSetOperation;
	friend class duckdb::LogicalAggregate;

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
	idx_t next_relation_ordinal = 0;
	identifier_set_t relation_aliases;
	vector<reference<LogicalOperator>> ancestors;
	vector<NamedRelation> named_relations;
};

} // namespace logical_plan_sql_export
} // namespace duckdb
