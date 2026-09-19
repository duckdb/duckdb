#pragma once

#include "duckdb/planner/bound_expression_sql_exporter.hpp"
#include "duckdb/planner/sql_export_helpers.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/parser/qualified_name.hpp"

namespace duckdb {

class BoundAggregateExpression;
class BoundCaseExpression;
class BoundColumnRefExpression;
class BoundConjunctionExpression;
class BoundConstantExpression;
class BoundFunctionExpression;
class BoundLambdaExpression;
class BoundOperatorExpression;
class BoundReferenceExpression;
class BoundUnnestExpression;
class BoundWindowExpression;

namespace bound_expression_sql_export {

using BoundExpressionSQLExportResult = LogicalPlanVerificationResult<unique_ptr<ParsedExpression>>;

using BoundAggregateSQLExportResult = LogicalPlanVerificationResult<unique_ptr<FunctionExpression>>;

using SQLExportHelpers::ChildPath;

using SQLExportHelpers::IsSQLRepresentableType;

using SQLExportHelpers::IsSQLValueType;

using SQLExportHelpers::IsValidIdentifier;

LogicalPlanVerificationIssue InternalInvariant(optional<LogicalPlanVerificationPath> path, string message,
                                               optional<LogicalPlanVerificationConstructIdentity> construct = {});

LogicalPlanVerificationIssue InternalExpressionInvariant(const LogicalPlanVerificationPath &path,
                                                         const Expression &expression, string message);

LogicalPlanVerificationIssue UnsupportedFeature(const LogicalPlanVerificationPath &path, string feature,
                                                string message);

LogicalPlanVerificationIssue UnsupportedFunction(const LogicalPlanVerificationPath &path,
                                                 LogicalPlanVerificationFunctionIdentity identity, string message);

BoundExpressionSQLExportResult Failure(LogicalPlanVerificationIssue issue);

bool HasNestedCollation(const LogicalType &type);

BoundExpressionSQLExportResult PreserveCollation(const LogicalType &type, BoundExpressionSQLExportResult result,
                                                 const LogicalPlanVerificationPath &path);

template <class FUNCTION>
static LogicalPlanVerificationFunctionIdentity DefinitionFunctionIdentity(const FUNCTION &definition,
                                                                          const vector<LogicalType> &arguments,
                                                                          const LogicalType &return_type) {
	LogicalPlanVerificationFunctionIdentity identity;
	identity.catalog = definition.GetCatalogName().GetIdentifierName();
	identity.schema = definition.GetSchemaName().GetIdentifierName();
	identity.name = definition.GetName().GetIdentifierName();
	identity.arguments = arguments;
	identity.return_type = return_type;
	return identity;
}

template <class FUNCTION>
static optional<QualifiedName> RebindableFunctionName(const FUNCTION &definition) {
	auto name =
	    QualifiedName(definition.GetCatalogName().empty() ? Identifier::SystemCatalog() : definition.GetCatalogName(),
	                  definition.GetSchemaName().empty() ? Identifier::DefaultSchema() : definition.GetSchemaName(),
	                  definition.GetName());
	if (name.Path().empty()) {
		return {};
	}
	for (auto &component : name.Path()) {
		if (!IsValidIdentifier(component)) {
			return {};
		}
	}
	return name;
}

LogicalType SQLCastType(const LogicalType &type);

unique_ptr<ParsedExpression> SQLCast(const LogicalType &type, unique_ptr<ParsedExpression> child,
                                     bool try_cast = false);

class BoundExpressionSQLExportState {
public:
	explicit BoundExpressionSQLExportState(const BoundExpressionSQLExportContext &context_p);
	BoundExpressionSQLExportResult Export(const Expression &expression, const LogicalPlanVerificationPath &path);
	BoundExpressionSQLExportResult ExportWindow(const BoundWindowExpression &expression,
	                                            const LogicalPlanVerificationPath &path);
	BoundExpressionSQLExportResult ExportUnnest(const BoundUnnestExpression &expression,
	                                            const LogicalPlanVerificationPath &path);
	BoundAggregateSQLExportResult ExportAggregateCall(const BoundAggregateExpression &expression,
	                                                  const LogicalPlanVerificationPath &path);

private:
	struct ChildExpression {
		explicit ChildExpression(optional_ptr<const Expression> expression_p,
		                         optional<LogicalType> expected_type_p = {})
		    : expression(expression_p), expected_type(std::move(expected_type_p)) {
		}

		optional_ptr<const Expression> expression;
		optional<LogicalType> expected_type;
	};
	BoundExpressionSQLExportResult ExportInternal(const Expression &expression,
	                                              const LogicalPlanVerificationPath &path);
	template <class FUNCTION>
	BoundExpressionSQLExportResult ExportWindowFunction(const BoundWindowExpression &expression,
	                                                    const FUNCTION &function,
	                                                    const LogicalPlanVerificationPath &path);
	BoundExpressionSQLExportResult RestoreResultType(const LogicalType &type, unique_ptr<ParsedExpression> result,
	                                                 const LogicalPlanVerificationPath &path);
	static bool RequiresConstantConstructor(const LogicalType &type);
	BoundExpressionSQLExportResult ExportNestedConstant(const LogicalType &type, optional_ptr<const Value> value,
	                                                    const LogicalPlanVerificationPath &path);
	BoundExpressionSQLExportResult CastToConstructedType(const LogicalType &type, unique_ptr<ParsedExpression> child,
	                                                     const LogicalPlanVerificationPath &path);
	BoundExpressionSQLExportResult ExportConstant(const BoundConstantExpression &expression,
	                                              const LogicalPlanVerificationPath &path);
	BoundExpressionSQLExportResult ExportReference(const BoundReferenceExpression &expression,
	                                               const LogicalPlanVerificationPath &path);
	BoundExpressionSQLExportResult ExportLambda(const BoundLambdaExpression &lambda,
	                                            const BoundFunctionExpression &function, idx_t logical_argument_count,
	                                            const LogicalPlanVerificationPath &path);
	BoundExpressionSQLExportResult ExportColumnRef(const BoundColumnRefExpression &expression,
	                                               const LogicalPlanVerificationPath &path);
	LogicalPlanVerificationIssue InvalidBinding(const LogicalPlanVerificationPath &path, const ColumnBinding &binding,
	                                            string message);
	BoundExpressionSQLExportResult ExportFunction(const BoundFunctionExpression &expression,
	                                              const LogicalPlanVerificationPath &path);
	BoundExpressionSQLExportResult ExportCast(const BoundFunctionExpression &expression,
	                                          const LogicalPlanVerificationPath &path);
	BoundExpressionSQLExportResult ExportComparison(const BoundFunctionExpression &expression,
	                                                const LogicalPlanVerificationPath &path);
	BoundExpressionSQLExportResult ExportBetween(const BoundFunctionExpression &expression,
	                                             const LogicalPlanVerificationPath &path);
	BoundExpressionSQLExportResult ExportConjunction(const BoundConjunctionExpression &expression,
	                                                 const LogicalPlanVerificationPath &path);
	BoundExpressionSQLExportResult ExportCase(const BoundCaseExpression &expression,
	                                          const LogicalPlanVerificationPath &path);
	BoundExpressionSQLExportResult ExportOperator(const BoundOperatorExpression &expression,
	                                              const LogicalPlanVerificationPath &path);
	BoundExpressionSQLExportResult CompressedMaterializationFailure(const BoundFunctionExpression &expression,
	                                                                const LogicalPlanVerificationPath &path,
	                                                                string message);
	optional<BoundExpressionSQLExportResult>
	TryExportCompressedMaterialization(const BoundFunctionExpression &expression,
	                                   const LogicalPlanVerificationPath &path);
	BoundExpressionSQLExportResult ExportScalarFunction(const BoundFunctionExpression &expression,
	                                                    const LogicalPlanVerificationPath &path);
	BoundAggregateSQLExportResult BuildAggregateCall(const BoundAggregateExpression &expression,
	                                                 const LogicalPlanVerificationPath &path);
	BoundExpressionSQLExportResult ExportAggregate(const BoundAggregateExpression &expression,
	                                               const LogicalPlanVerificationPath &path);
	BoundExpressionSQLExportResult ExportChild(const Expression &expression, const LogicalPlanVerificationPath &path,
	                                           idx_t child_index);
	void ExportChildren(const vector<unique_ptr<Expression>> &source, const LogicalPlanVerificationPath &path,
	                    vector<unique_ptr<ParsedExpression>> &result, vector<LogicalPlanVerificationIssue> &issues,
	                    const optional<LogicalType> &expected_type = {});
	void ExportChildren(const vector<ChildExpression> &source, const LogicalPlanVerificationPath &path,
	                    vector<unique_ptr<ParsedExpression>> &result, vector<LogicalPlanVerificationIssue> &issues);
	const BoundExpressionSQLExportContext &context;
	vector<vector<unique_ptr<ParsedExpression>>> lambda_reference_scopes;
};

} // namespace bound_expression_sql_export
} // namespace duckdb
