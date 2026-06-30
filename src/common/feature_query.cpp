#include "duckdb/common/feature_query.hpp"

#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/sql_identifier.hpp"
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
	if (!parameters.spine_filter.empty()) {
		throw InternalException("BuildFeaturePITQuery does not support SQL string filters; use anchor_filter");
	}

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
		result_select->groups.group_expressions.push_back(FeatureColumnRef("anchor", entity_column));

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
	result_select->groups.group_expressions.push_back(FeatureColumnRef("anchor", "feature_timestamp"));
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

string BuildFeaturePITQuerySQL(const SelectNode &select_node, const FeaturePITQueryParameters &parameters) {
	string agg_exprs;
	for (auto &expr : select_node.select_list) {
		if (expr->GetExpressionClass() == ExpressionClass::COLUMN_REF) {
			auto &col_ref = expr->Cast<ColumnRefExpression>();
			if (FeatureColumnListContains(parameters.entity_columns, col_ref.GetColumnName())) {
				continue;
			}
		}
		if (!agg_exprs.empty()) {
			agg_exprs += ", ";
		}
		agg_exprs += expr->ToString();
		if (expr->HasAlias()) {
			agg_exprs += " AS " + SQLIdentifier::ToString(expr->GetAlias());
		}
	}
	if (agg_exprs.empty()) {
		throw BinderException("CREATE FEATURE query must project at least one feature expression");
	}

	vector<string> anchor_entity_selects;
	vector<string> anchor_entity_outputs;
	vector<string> entity_join_conditions;
	vector<string> group_by_columns;
	vector<string> order_by_columns;
	auto table = SQLIdentifier::ToString(parameters.source_table);
	for (auto &entity_column : parameters.entity_columns) {
		auto entity = SQLIdentifier::ToString(entity_column);
		anchor_entity_selects.push_back(entity);
		anchor_entity_outputs.push_back("anchor." + entity);
		entity_join_conditions.push_back(table + "." + entity + " = anchor." + entity);
		group_by_columns.push_back("anchor." + entity);
		order_by_columns.push_back("anchor." + entity);
	}
	group_by_columns.push_back("anchor.feature_timestamp");
	order_by_columns.push_back("anchor.feature_timestamp");

	auto ts = SQLIdentifier::ToString(parameters.timestamp_column);
	auto window = Interval::ToString(parameters.window_interval);
	auto anchor_select = StringUtil::Join(anchor_entity_selects, ", ");
	if (!anchor_select.empty()) {
		anchor_select += ", ";
	}
	anchor_select += ts + " AS feature_timestamp";

	auto output_columns = StringUtil::Join(anchor_entity_outputs, ", ");
	if (!output_columns.empty()) {
		output_columns += ", ";
	}
	output_columns += "anchor.feature_timestamp, " + agg_exprs;

	entity_join_conditions.push_back(table + "." + ts + " <= anchor.feature_timestamp");
	entity_join_conditions.push_back(table + "." + ts + " >= anchor.feature_timestamp - INTERVAL '" + window + "'");

	string pit_sql =
	    StringUtil::Format("SELECT %s "
	                       "FROM (SELECT %s FROM %s%s) AS anchor "
	                       "JOIN %s ON %s "
	                       "GROUP BY %s",
	                       output_columns, anchor_select, table, parameters.spine_filter, table,
	                       StringUtil::Join(entity_join_conditions, " AND "), StringUtil::Join(group_by_columns, ", "));
	if (parameters.order_result) {
		pit_sql += " ORDER BY " + StringUtil::Join(order_by_columns, ", ");
	}
	return pit_sql;
}

} // namespace duckdb
