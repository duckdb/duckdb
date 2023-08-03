//===----------------------------------------------------------------------===//
//                         DuckDB
//
// statement_generator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/parser/query_node.hpp"

namespace duckdb {
class SQLStatement;
class SelectStatement;
class InsertStatement;
class UpdateStatement;
class DeleteStatement;
class TableRef;
class SelectNode;
class SetOperationNode;
class QueryNode;
class ParsedExpression;
class ResultModifier;
class OrderModifier;
class UpdateSetInfo;

struct GeneratorContext;

class StatementGenerator {
public:
	constexpr static idx_t MAX_DEPTH = 10;
	constexpr static idx_t MAX_EXPRESSION_DEPTH = 50;

	friend class ExpressionDepthChecker;
	friend class AggregateChecker;
	friend class WindowChecker;

public:
	StatementGenerator(ClientContext &context);
	StatementGenerator(StatementGenerator &parent);
	~StatementGenerator();

public:
	unique_ptr<SQLStatement> GenerateStatement();

	vector<string> GenerateAllFunctionCalls();

private:
	unique_ptr<SQLStatement> GenerateStatement(StatementType type);

	unique_ptr<SQLStatement> GenerateSelect();
	unique_ptr<QueryNode> GenerateQueryNode();

	void GenerateCTEs(QueryNode &node);
	unique_ptr<TableRef> GenerateTableRef();
	unique_ptr<ParsedExpression> GenerateExpression();

	unique_ptr<TableRef> GenerateBaseTableRef();
	unique_ptr<TableRef> GenerateExpressionListRef();
	unique_ptr<TableRef> GenerateJoinRef();
	unique_ptr<TableRef> GenerateSubqueryRef();
	unique_ptr<TableRef> GenerateTableFunctionRef();
	unique_ptr<TableRef> GeneratePivotRef();

	unique_ptr<ParsedExpression> GenerateConstant();
	unique_ptr<ParsedExpression> GenerateColumnRef();
	unique_ptr<ParsedExpression> GenerateFunction();
	unique_ptr<ParsedExpression> GenerateOperator();
	unique_ptr<ParsedExpression> GenerateWindowFunction(optional_ptr<AggregateFunction> function = nullptr);
	unique_ptr<ParsedExpression> GenerateConjunction();
	unique_ptr<ParsedExpression> GenerateStar();
	unique_ptr<ParsedExpression> GenerateLambda();
	unique_ptr<ParsedExpression> GenerateSubquery();
	unique_ptr<ParsedExpression> GenerateCast();
	unique_ptr<ParsedExpression> GenerateBetween();
	unique_ptr<ParsedExpression> GenerateComparison();
	unique_ptr<ParsedExpression> GeneratePositionalReference();
	unique_ptr<ParsedExpression> GenerateCase();

	unique_ptr<OrderModifier> GenerateOrderBy();

	LogicalType GenerateLogicalType();

	void GenerateAllScalar(ScalarFunctionCatalogEntry &scalar_function, vector<string> &result);
	void GenerateAllAggregate(AggregateFunctionCatalogEntry &aggregate_function, vector<string> &result);
	string GenerateTestAllTypes(BaseScalarFunction &base_function);
	string GenerateTestVectorTypes(BaseScalarFunction &base_function);
	string GenerateCast(const LogicalType &target, const string &source_name, bool add_varchar);
	bool FunctionArgumentsAlwaysNull(const string &name);

	idx_t RandomValue(idx_t max);
	bool RandomBoolean();
	//! Returns true with a percentage change (0-100)
	bool RandomPercentage(idx_t percentage);
	unique_ptr<ParsedExpression> RandomExpression(idx_t percentage);

	string GenerateIdentifier();
	string GenerateTableIdentifier();

	string GenerateRelationName();
	string GenerateColumnName();
	idx_t GetIndex();

	Value GenerateConstantValue();

	ExpressionType GenerateComparisonType();

private:
	ClientContext &context;
	optional_ptr<StatementGenerator> parent;
	unique_ptr<SQLStatement> current_statement;
	vector<string> current_relation_names;
	vector<string> current_column_names;

	shared_ptr<GeneratorContext> generator_context;
	idx_t index = 0;
	idx_t depth = 0;
	idx_t expression_depth = 0;

	bool in_window = false;
	bool in_aggregate = false;

	shared_ptr<GeneratorContext> GetDatabaseState(ClientContext &context);
	vector<unique_ptr<ParsedExpression>> GenerateChildren(idx_t min, idx_t max);

	template <class T>
	const T &Choose(const vector<T> &entries) {
		if (entries.empty()) {
			throw InternalException("Attempting to choose from an empty vector");
		}
		return entries[RandomValue(entries.size())];
	}
};

} // namespace duckdb
