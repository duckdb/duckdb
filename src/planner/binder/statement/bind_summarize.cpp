#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/statement/show_statement.hpp"
#include "duckdb/planner/operator/logical_show.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"

namespace duckdb {

static unique_ptr<ParsedExpression> SummarizeWrapUnnest(vector<unique_ptr<ParsedExpression>> &children,
                                                        const string &alias) {
	auto list_function = make_unique<FunctionExpression>("list_value", move(children));
	vector<unique_ptr<ParsedExpression>> unnest_children;
	unnest_children.push_back(move(list_function));
	auto unnest_function = make_unique<FunctionExpression>("unnest", move(unnest_children));
	unnest_function->alias = alias;
	return move(unnest_function);
}

static unique_ptr<ParsedExpression> SummarizeCreateAggregate(const string &aggregate, string column_name) {
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(make_unique<ColumnRefExpression>(move(column_name)));
	auto aggregate_function = make_unique<FunctionExpression>(aggregate, move(children));
	auto cast_function = make_unique<CastExpression>(LogicalType::VARCHAR, move(aggregate_function));
	return move(cast_function);
}

static unique_ptr<ParsedExpression> SummarizeCreateAggregate(const string &aggregate, string column_name,
                                                             const Value &modifier) {
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(make_unique<ColumnRefExpression>(move(column_name)));
	children.push_back(make_unique<ConstantExpression>(modifier));
	auto aggregate_function = make_unique<FunctionExpression>(aggregate, move(children));
	auto cast_function = make_unique<CastExpression>(LogicalType::VARCHAR, move(aggregate_function));
	return move(cast_function);
}

static unique_ptr<ParsedExpression> SummarizeCreateCountStar() {
	vector<unique_ptr<ParsedExpression>> children;
	auto aggregate_function = make_unique<FunctionExpression>("count_star", move(children));
	return move(aggregate_function);
}

static unique_ptr<ParsedExpression> SummarizeCreateBinaryFunction(const string &op, unique_ptr<ParsedExpression> left,
                                                                  unique_ptr<ParsedExpression> right) {
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(move(left));
	children.push_back(move(right));
	auto binary_function = make_unique<FunctionExpression>(op, move(children));
	return move(binary_function);
}

static unique_ptr<ParsedExpression> SummarizeCreateNullPercentage(string column_name) {
	auto count_star = make_unique<CastExpression>(LogicalType::DOUBLE, SummarizeCreateCountStar());
	auto count = make_unique<CastExpression>(LogicalType::DOUBLE, SummarizeCreateAggregate("count", move(column_name)));
	auto null_percentage = SummarizeCreateBinaryFunction("/", move(count), move(count_star));
	auto negate_x =
	    SummarizeCreateBinaryFunction("-", make_unique<ConstantExpression>(Value::DOUBLE(1)), move(null_percentage));
	auto percentage_x =
	    SummarizeCreateBinaryFunction("*", move(negate_x), make_unique<ConstantExpression>(Value::DOUBLE(100)));
	auto round_x =
	    SummarizeCreateBinaryFunction("round", move(percentage_x), make_unique<ConstantExpression>(Value::INTEGER(2)));
	auto concat_x = SummarizeCreateBinaryFunction("concat", move(round_x), make_unique<ConstantExpression>(Value("%")));

	return concat_x;
}

BoundStatement Binder::BindSummarize(ShowStatement &stmt) {
	auto query_copy = stmt.info->query->Copy();

	// we bind the plan once in a child-node to figure out the column names and column types
	auto child_binder = Binder::CreateBinder(context);
	auto plan = child_binder->Bind(*stmt.info->query);
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
	auto select = make_unique<SelectStatement>();
	select->node = move(query_copy);
	for (idx_t i = 0; i < plan.names.size(); i++) {
		name_children.push_back(make_unique<ConstantExpression>(Value(plan.names[i])));
		type_children.push_back(make_unique<ConstantExpression>(Value(plan.types[i].ToString())));
		min_children.push_back(SummarizeCreateAggregate("min", plan.names[i]));
		max_children.push_back(SummarizeCreateAggregate("max", plan.names[i]));
		unique_children.push_back(SummarizeCreateAggregate("approx_count_distinct", plan.names[i]));
		if (plan.types[i].IsNumeric()) {
			avg_children.push_back(SummarizeCreateAggregate("avg", plan.names[i]));
			std_children.push_back(SummarizeCreateAggregate("stddev", plan.names[i]));
			q25_children.push_back(SummarizeCreateAggregate("approx_quantile", plan.names[i], Value::FLOAT(0.25)));
			q50_children.push_back(SummarizeCreateAggregate("approx_quantile", plan.names[i], Value::FLOAT(0.50)));
			q75_children.push_back(SummarizeCreateAggregate("approx_quantile", plan.names[i], Value::FLOAT(0.75)));
		} else {
			avg_children.push_back(make_unique<ConstantExpression>(Value()));
			std_children.push_back(make_unique<ConstantExpression>(Value()));
			q25_children.push_back(make_unique<ConstantExpression>(Value()));
			q50_children.push_back(make_unique<ConstantExpression>(Value()));
			q75_children.push_back(make_unique<ConstantExpression>(Value()));
		}
		count_children.push_back(SummarizeCreateCountStar());
		null_percentage_children.push_back(SummarizeCreateNullPercentage(plan.names[i]));
	}
	auto subquery_ref = make_unique<SubqueryRef>(move(select), "summarize_tbl");
	subquery_ref->column_name_alias = plan.names;

	auto select_node = make_unique<SelectNode>();
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
	select_node->from_table = move(subquery_ref);

	properties.return_type = StatementReturnType::QUERY_RESULT;
	return Bind(*select_node);
}

} // namespace duckdb
