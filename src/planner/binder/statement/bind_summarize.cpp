#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/case_expression.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/tableref/showref.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/expression/star_expression.hpp"

namespace duckdb {

static unique_ptr<ParsedExpression> SummarizeWrapUnnest(vector<unique_ptr<ParsedExpression>> &children,
                                                        const string &alias) {
	auto list_function = make_uniq<FunctionExpression>("list_value", std::move(children));
	vector<unique_ptr<ParsedExpression>> unnest_children;
	unnest_children.push_back(std::move(list_function));
	auto unnest_function = make_uniq<FunctionExpression>("unnest", std::move(unnest_children));
	unnest_function->SetAlias(Identifier(alias));
	return std::move(unnest_function);
}

static unique_ptr<ParsedExpression> SummarizeCreateUnaryFunction(const string &function,
                                                                 unique_ptr<ParsedExpression> child) {
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(std::move(child));
	return make_uniq<FunctionExpression>(Identifier(function), std::move(children));
}

static unique_ptr<ParsedExpression> SummarizeCreateBinaryFunction(const string &op, unique_ptr<ParsedExpression> left,
                                                                  unique_ptr<ParsedExpression> right) {
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(std::move(left));
	children.push_back(std::move(right));
	auto binary_function = make_uniq<FunctionExpression>(Identifier(op), std::move(children));
	return std::move(binary_function);
}

// Rounds to a minimum of 3 (unless trailing zeros) and max of 10 significant digits via printf's shortest
// representation %g. Builds: printf('%.*g', LEAST(GREATEST(FLOOR(LOG10(ABS(value::DOUBLE))) + 1, 3), 10)::INTEGER,
// value::DOUBLE)
static unique_ptr<ParsedExpression> SummarizeCreateRoundedValue(unique_ptr<ParsedExpression> value) {
	auto double_value = make_uniq<CastExpression>(LogicalType::DOUBLE, std::move(value));
	auto order_of_magnitude = SummarizeCreateUnaryFunction(
	    "floor", SummarizeCreateUnaryFunction("log10", SummarizeCreateUnaryFunction("abs", double_value->Copy())));
	auto full_precision =
	    SummarizeCreateBinaryFunction("greatest",
	                                  SummarizeCreateBinaryFunction("+", std::move(order_of_magnitude),
	                                                                make_uniq<ConstantExpression>(Value::DOUBLE(1))),
	                                  make_uniq<ConstantExpression>(Value::DOUBLE(3)));
	auto capped_precision = SummarizeCreateBinaryFunction("least", std::move(full_precision),
	                                                      make_uniq<ConstantExpression>(Value::DOUBLE(10)));

	vector<unique_ptr<ParsedExpression>> printf_children;
	printf_children.push_back(make_uniq<ConstantExpression>(Value("%.*g")));
	printf_children.push_back(make_uniq<CastExpression>(LogicalType::INTEGER, std::move(capped_precision)));
	printf_children.push_back(std::move(double_value));
	return make_uniq<FunctionExpression>("printf", std::move(printf_children));
}

static unique_ptr<ParsedExpression>
SummarizeWrapAggregate(const string &aggregate, vector<unique_ptr<ParsedExpression>> children, bool round_result) {
	unique_ptr<ParsedExpression> aggregate_function =
	    make_uniq<FunctionExpression>(Identifier(aggregate), std::move(children));
	if (round_result) {
		return SummarizeCreateRoundedValue(std::move(aggregate_function));
	}
	return make_uniq<CastExpression>(LogicalType::VARCHAR, std::move(aggregate_function));
}

static unique_ptr<ParsedExpression> SummarizeCreateAggregate(const string &aggregate, Identifier column_name,
                                                             const bool round_result = false) {
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(make_uniq<ColumnRefExpression>(std::move(column_name)));
	return SummarizeWrapAggregate(aggregate, std::move(children), round_result);
}

static unique_ptr<ParsedExpression> SummarizeCreateAggregate(const string &aggregate, Identifier column_name,
                                                             const Value &modifier, const bool round_result = false) {
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(make_uniq<ColumnRefExpression>(std::move(column_name)));
	children.push_back(make_uniq<ConstantExpression>(modifier));
	return SummarizeWrapAggregate(aggregate, std::move(children), round_result);
}

static unique_ptr<ParsedExpression> SummarizeCreateCountStar() {
	vector<unique_ptr<ParsedExpression>> children;
	auto aggregate_function = make_uniq<FunctionExpression>("count_star", std::move(children));
	return std::move(aggregate_function);
}

static unique_ptr<ParsedExpression> SummarizeCreateNullPercentage(Identifier column_name) {
	auto count_star = make_uniq<CastExpression>(LogicalType::DOUBLE, SummarizeCreateCountStar());
	auto count =
	    make_uniq<CastExpression>(LogicalType::DOUBLE, SummarizeCreateAggregate("count", std::move(column_name)));
	auto null_percentage = SummarizeCreateBinaryFunction("/", std::move(count), std::move(count_star));
	auto negate_x =
	    SummarizeCreateBinaryFunction("-", make_uniq<ConstantExpression>(Value::DOUBLE(1)), std::move(null_percentage));
	auto percentage_x =
	    SummarizeCreateBinaryFunction("*", std::move(negate_x), make_uniq<ConstantExpression>(Value::DOUBLE(100)));

	auto comp_expr = make_uniq<ComparisonExpression>(ExpressionType::COMPARE_GREATERTHAN, SummarizeCreateCountStar(),
	                                                 make_uniq<ConstantExpression>(Value::BIGINT(0)));
	auto case_expr = make_uniq<CaseExpression>();
	CaseCheck check;
	check.when_expr = std::move(comp_expr);
	check.then_expr = std::move(percentage_x);
	case_expr->CaseChecksMutable().push_back(std::move(check));
	case_expr->ElseMutable() = make_uniq<ConstantExpression>(Value());

	return make_uniq<CastExpression>(LogicalType::DECIMAL(9, 2), std::move(case_expr));
}

BoundStatement Binder::BindSummarize(ShowRef &ref) {
	unique_ptr<QueryNode> query;
	if (ref.query) {
		query = std::move(ref.query);
	} else {
		auto table_name = QualifiedName::Parse(ref.GetTableName().GetIdentifierName());
		auto node = make_uniq<SelectNode>();
		node->select_list.push_back(make_uniq<StarExpression>());
		auto basetableref = make_uniq<BaseTableRef>();
		basetableref->SetQualifiedName(table_name);
		node->from_table = std::move(basetableref);
		query = std::move(node);
	}
	auto query_copy = query->Copy();

	// we bind the plan once in a child-node to figure out the column names and column types
	auto child_binder = Binder::CreateBinder(context, this);
	auto plan = child_binder->Bind(*query);
	D_ASSERT(plan.types.size() == plan.names.size());
	vector<unique_ptr<ParsedExpression>> name_children;
	vector<unique_ptr<ParsedExpression>> type_children;
	vector<unique_ptr<ParsedExpression>> min_children;
	vector<unique_ptr<ParsedExpression>> max_children;
	vector<unique_ptr<ParsedExpression>> unique_children;
	vector<unique_ptr<ParsedExpression>> avg_children;
	vector<unique_ptr<ParsedExpression>> std_children;
	vector<unique_ptr<ParsedExpression>> q25_children;
	vector<unique_ptr<ParsedExpression>> q50_children;
	vector<unique_ptr<ParsedExpression>> q75_children;
	vector<unique_ptr<ParsedExpression>> count_children;
	vector<unique_ptr<ParsedExpression>> null_percentage_children;
	auto select = make_uniq<SelectStatement>();
	select->node = std::move(query_copy);
	for (idx_t i = 0; i < plan.names.size(); i++) {
		name_children.push_back(make_uniq<ConstantExpression>(Value(plan.names[i])));
		type_children.push_back(make_uniq<ConstantExpression>(Value(plan.types[i].ToString())));
		min_children.push_back(SummarizeCreateAggregate("min", plan.names[i]));
		max_children.push_back(SummarizeCreateAggregate("max", plan.names[i]));
		unique_children.push_back(make_uniq<CastExpression>(
		    LogicalType::BIGINT, SummarizeCreateAggregate("approx_count_distinct", plan.names[i])));
		if (plan.types[i].IsNumeric()) {
			avg_children.push_back(SummarizeCreateAggregate("avg", plan.names[i], true));
		} else if (plan.types[i].IsTemporal()) {
			avg_children.push_back(SummarizeCreateAggregate("avg", plan.names[i]));
		} else {
			avg_children.push_back(make_uniq<ConstantExpression>(Value()));
		}
		if (plan.types[i].IsNumeric()) {
			std_children.push_back(SummarizeCreateAggregate("stddev", plan.names[i], true));
		} else {
			std_children.push_back(make_uniq<ConstantExpression>(Value()));
		}
		if (plan.types[i].IsNumeric() || plan.types[i].IsTemporal()) {
			auto round_quantile = plan.types[i].IsNumeric();
			q25_children.push_back(
			    SummarizeCreateAggregate("approx_quantile", plan.names[i], Value::FLOAT(0.25), round_quantile));
			q50_children.push_back(
			    SummarizeCreateAggregate("approx_quantile", plan.names[i], Value::FLOAT(0.50), round_quantile));
			q75_children.push_back(
			    SummarizeCreateAggregate("approx_quantile", plan.names[i], Value::FLOAT(0.75), round_quantile));
		} else {
			q25_children.push_back(make_uniq<ConstantExpression>(Value()));
			q50_children.push_back(make_uniq<ConstantExpression>(Value()));
			q75_children.push_back(make_uniq<ConstantExpression>(Value()));
		}
		count_children.push_back(SummarizeCreateCountStar());
		null_percentage_children.push_back(SummarizeCreateNullPercentage(plan.names[i]));
	}
	auto subquery_ref = make_uniq<SubqueryRef>(std::move(select), "summarize_tbl");
	subquery_ref->column_name_alias = plan.names;

	auto select_node = make_uniq<SelectNode>();
	select_node->select_list.push_back(SummarizeWrapUnnest(name_children, "column_name"));
	select_node->select_list.push_back(SummarizeWrapUnnest(type_children, "column_type"));
	select_node->select_list.push_back(SummarizeWrapUnnest(min_children, "min"));
	select_node->select_list.push_back(SummarizeWrapUnnest(max_children, "max"));
	select_node->select_list.push_back(SummarizeWrapUnnest(unique_children, "approx_unique"));
	select_node->select_list.push_back(SummarizeWrapUnnest(avg_children, "avg"));
	select_node->select_list.push_back(SummarizeWrapUnnest(std_children, "std"));
	select_node->select_list.push_back(SummarizeWrapUnnest(q25_children, "q25"));
	select_node->select_list.push_back(SummarizeWrapUnnest(q50_children, "q50"));
	select_node->select_list.push_back(SummarizeWrapUnnest(q75_children, "q75"));
	select_node->select_list.push_back(SummarizeWrapUnnest(count_children, "count"));
	select_node->select_list.push_back(SummarizeWrapUnnest(null_percentage_children, "null_percentage"));
	select_node->from_table = std::move(subquery_ref);

	auto select_stmt = make_uniq<SelectStatement>();
	select_stmt->node = std::move(select_node);
	auto subquery = make_uniq<SubqueryRef>(std::move(select_stmt));
	return Bind(*subquery);
}

} // namespace duckdb
