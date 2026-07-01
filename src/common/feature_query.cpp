#include "duckdb/common/feature_query.hpp"

#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/result_modifier.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"

namespace duckdb {

static unique_ptr<BaseTableRef> FeatureBaseTable(const string &table_name) {
	auto result = make_uniq<BaseTableRef>();
	result->table_name = table_name;
	return result;
}

static unique_ptr<ColumnRefExpression> FeatureColumnRef(const string &column_name) {
	return make_uniq<ColumnRefExpression>(column_name);
}

static unique_ptr<ColumnRefExpression> FeatureColumnRef(const string &table_name, const string &column_name) {
	return make_uniq<ColumnRefExpression>(column_name, table_name);
}

static unique_ptr<ParsedExpression> FeatureConjoin(unique_ptr<ParsedExpression> left,
                                                   unique_ptr<ParsedExpression> right) {
	return make_uniq<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(left), std::move(right));
}

static unique_ptr<ParsedExpression> FeatureBinaryFunction(const string &function_name,
                                                          unique_ptr<ParsedExpression> left,
                                                          unique_ptr<ParsedExpression> right) {
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(std::move(left));
	children.push_back(std::move(right));
	return make_uniq<FunctionExpression>(function_name, std::move(children), nullptr, nullptr, false, true);
}

static void FeatureAddGroupExpression(SelectNode &select_node, unique_ptr<ParsedExpression> expression) {
	auto group_index = ProjectionIndex(select_node.groups.group_expressions.size());
	select_node.groups.group_expressions.push_back(std::move(expression));
	if (select_node.groups.grouping_sets.empty()) {
		select_node.groups.grouping_sets.emplace_back();
	}
	select_node.groups.grouping_sets[0].insert(group_index);
}

bool FeatureColumnListContains(const vector<string> &columns, const string &column_name) {
	for (auto &column : columns) {
		if (StringUtil::CIEquals(column, column_name)) {
			return true;
		}
	}
	return false;
}

unique_ptr<SelectStatement> BuildFeaturePITQuery(const SelectNode &select_node,
                                                 const FeaturePITQueryParameters &parameters) {
	vector<unique_ptr<ParsedExpression>> feature_expressions;
	for (auto &expr : select_node.select_list) {
		if (expr->GetExpressionClass() == ExpressionClass::COLUMN_REF) {
			auto &col_ref = expr->Cast<ColumnRefExpression>();
			if (FeatureColumnListContains(parameters.entity_columns, col_ref.GetColumnName())) {
				continue;
			}
		}
		feature_expressions.push_back(expr->Copy());
	}
	if (feature_expressions.empty()) {
		throw BinderException("CREATE FEATURE query must project at least one feature expression");
	}

	auto anchor_select = make_uniq<SelectNode>();
	auto result_select = make_uniq<SelectNode>();
	unique_ptr<ParsedExpression> join_condition;
	for (auto &entity_column : parameters.entity_columns) {
		anchor_select->select_list.push_back(FeatureColumnRef(entity_column));
		result_select->select_list.push_back(FeatureColumnRef("anchor", entity_column));
		FeatureAddGroupExpression(*result_select, FeatureColumnRef("anchor", entity_column));

		auto entity_condition = make_uniq<ComparisonExpression>(
		    ExpressionType::COMPARE_EQUAL, FeatureColumnRef(parameters.source_table, entity_column),
		    FeatureColumnRef("anchor", entity_column));
		join_condition = join_condition ? FeatureConjoin(std::move(join_condition), std::move(entity_condition))
		                                : std::move(entity_condition);
	}

	auto anchor_timestamp = FeatureColumnRef(parameters.timestamp_column);
	anchor_timestamp->SetAlias("feature_timestamp");
	anchor_select->select_list.push_back(std::move(anchor_timestamp));
	anchor_select->from_table = FeatureBaseTable(parameters.source_table);
	if (parameters.anchor_filter) {
		anchor_select->where_clause = parameters.anchor_filter->Copy();
	}

	result_select->select_list.push_back(FeatureColumnRef("anchor", "feature_timestamp"));
	FeatureAddGroupExpression(*result_select, FeatureColumnRef("anchor", "feature_timestamp"));
	for (auto &feature_expression : feature_expressions) {
		result_select->select_list.push_back(std::move(feature_expression));
	}

	auto timestamp_upper_bound =
	    make_uniq<ComparisonExpression>(ExpressionType::COMPARE_LESSTHANOREQUALTO,
	                                    FeatureColumnRef(parameters.source_table, parameters.timestamp_column),
	                                    FeatureColumnRef("anchor", "feature_timestamp"));
	join_condition = join_condition ? FeatureConjoin(std::move(join_condition), std::move(timestamp_upper_bound))
	                                : std::move(timestamp_upper_bound);

	auto window_start =
	    FeatureBinaryFunction("-", FeatureColumnRef("anchor", "feature_timestamp"),
	                          make_uniq<ConstantExpression>(Value::INTERVAL(parameters.window_interval)));
	auto timestamp_lower_bound = make_uniq<ComparisonExpression>(
	    ExpressionType::COMPARE_GREATERTHANOREQUALTO,
	    FeatureColumnRef(parameters.source_table, parameters.timestamp_column), std::move(window_start));
	join_condition = FeatureConjoin(std::move(join_condition), std::move(timestamp_lower_bound));

	auto anchor_statement = make_uniq<SelectStatement>();
	anchor_statement->node = std::move(anchor_select);
	auto anchor_ref = make_uniq<SubqueryRef>(std::move(anchor_statement), "anchor");

	auto join = make_uniq<JoinRef>();
	join->left = std::move(anchor_ref);
	join->right = FeatureBaseTable(parameters.source_table);
	join->condition = std::move(join_condition);
	result_select->from_table = std::move(join);

	if (parameters.order_result) {
		auto order = make_uniq<OrderModifier>();
		for (auto &entity_column : parameters.entity_columns) {
			order->orders.emplace_back(OrderType::ASCENDING, OrderByNullType::ORDER_DEFAULT,
			                           FeatureColumnRef("anchor", entity_column));
		}
		order->orders.emplace_back(OrderType::ASCENDING, OrderByNullType::ORDER_DEFAULT,
		                           FeatureColumnRef("anchor", "feature_timestamp"));
		result_select->modifiers.push_back(std::move(order));
	}

	auto result = make_uniq<SelectStatement>();
	result->node = std::move(result_select);
	return result;
}

} // namespace duckdb
