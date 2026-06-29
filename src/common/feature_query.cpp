#include "duckdb/common/feature_query.hpp"

#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/sql_identifier.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"

namespace duckdb {

bool FeatureColumnListContains(const vector<string> &columns, const string &column_name) {
	for (auto &column : columns) {
		if (StringUtil::CIEquals(column, column_name)) {
			return true;
		}
	}
	return false;
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
