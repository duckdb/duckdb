#include "duckdb/common/feature_query.hpp"

#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/result_modifier.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"

namespace duckdb {

static unique_ptr<BaseTableRef> FeatureBaseTable(const string &table_name, const string &alias) {
	auto result = make_uniq<BaseTableRef>();
	result->table_name = table_name;
	result->alias = alias;
	return result;
}

static unique_ptr<ColumnRefExpression> FeatureColumnRef(const string &column_name);

//! (SELECT DISTINCT <key columns> FROM <entity_table>) aliased "entity". DISTINCT guarantees one row per
//! entity even when the entity table has no primary key (or duplicate key rows), so the snapshot's LEFT JOIN
//! does not fan out.
static unique_ptr<SubqueryRef> FeatureDistinctEntitySource(const string &entity_table,
                                                           const vector<string> &entity_key_columns) {
	auto select = make_uniq<SelectNode>();
	for (auto &key_column : entity_key_columns) {
		select->select_list.push_back(FeatureColumnRef(key_column));
	}
	select->modifiers.push_back(make_uniq<DistinctModifier>());
	select->from_table = FeatureBaseTable(entity_table, string());
	auto statement = make_uniq<SelectStatement>();
	statement->node = std::move(select);
	return make_uniq<SubqueryRef>(std::move(statement), "entity");
}

static unique_ptr<ColumnRefExpression> FeatureColumnRef(const string &column_name) {
	return make_uniq<ColumnRefExpression>(column_name);
}

static unique_ptr<ColumnRefExpression> FeatureColumnRef(const string &table_alias, const string &column_name) {
	return make_uniq<ColumnRefExpression>(column_name, table_alias);
}

static unique_ptr<ParsedExpression> FeatureConjoin(unique_ptr<ParsedExpression> left,
                                                   unique_ptr<ParsedExpression> right) {
	if (!left) {
		return right;
	}
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

//! Whether the feature expression's default value on an unmatched entity should be 0 instead of NULL.
//! Only direct SUM/COUNT aggregates have a natural zero default.
static bool FeatureDefaultsToZero(const ParsedExpression &expr) {
	if (expr.GetExpressionClass() != ExpressionClass::FUNCTION) {
		return false;
	}
	auto &func = expr.Cast<FunctionExpression>();
	return StringUtil::CIEquals(func.function_name, "sum") || StringUtil::CIEquals(func.function_name, "count") ||
	       StringUtil::CIEquals(func.function_name, "count_star");
}

static bool FeatureIsEntityColumn(const ParsedExpression &expr, const vector<string> &entity_columns) {
	if (expr.GetExpressionClass() != ExpressionClass::COLUMN_REF) {
		return false;
	}
	auto &col_ref = expr.Cast<ColumnRefExpression>();
	return FeatureColumnListContains(entity_columns, col_ref.GetColumnName());
}

//! A reference to the feature timestamp column, qualified with its table alias when one was declared
//! (TIMESTAMP tbl.col), so the predicate resolves unambiguously in an arbitrary FROM (joins, CTEs).
static unique_ptr<ColumnRefExpression> FeatureTimestampRef(const FeatureSnapshotParameters &parameters) {
	if (parameters.timestamp_table.empty()) {
		return FeatureColumnRef(parameters.timestamp_column);
	}
	return FeatureColumnRef(parameters.timestamp_table, parameters.timestamp_column);
}

//! ts_col >= feature_ts - window  AND  ts_col < feature_ts
static unique_ptr<ParsedExpression> FeatureWindowFilter(const FeatureSnapshotParameters &parameters) {
	auto ts_value = Value::TIMESTAMP(parameters.feature_ts);
	auto window_start =
	    FeatureBinaryFunction("-", make_uniq<ConstantExpression>(ts_value),
	                          make_uniq<ConstantExpression>(Value::INTERVAL(parameters.window_interval)));
	auto lower = make_uniq<ComparisonExpression>(ExpressionType::COMPARE_GREATERTHANOREQUALTO,
	                                             FeatureTimestampRef(parameters), std::move(window_start));
	auto upper = make_uniq<ComparisonExpression>(ExpressionType::COMPARE_LESSTHAN, FeatureTimestampRef(parameters),
	                                             make_uniq<ConstantExpression>(ts_value));
	return FeatureConjoin(std::move(lower), std::move(upper));
}

namespace {
//! Metadata captured for each projected feature expression before it is re-aliased in the inner query.
struct FeatureColumnInfo {
	string internal_alias; //! The stable alias used inside the aggregate subquery (e.g. "__f0").
	string output_name;    //! The name the feature column should keep in the snapshot output.
	bool defaults_to_zero; //! Whether unmatched entities should COALESCE to 0.
};
} // namespace

unique_ptr<SelectStatement> BuildFeatureSnapshotQuery(const SelectNode &select_node,
                                                      const FeatureSnapshotParameters &parameters) {
	// The inner aggregate subquery is the user's feature query with a window filter applied. Copy it so we
	// can re-alias its feature expressions to stable internal names and AND in the time predicate.
	auto inner_node_base = select_node.Copy();
	auto &inner_node = inner_node_base->Cast<SelectNode>();

	vector<FeatureColumnInfo> feature_columns;
	idx_t feature_index = 0;
	for (auto &expr : inner_node.select_list) {
		if (FeatureIsEntityColumn(*expr, parameters.entity_columns)) {
			continue;
		}
		FeatureColumnInfo info;
		info.internal_alias = "__f" + to_string(feature_index++);
		info.output_name = expr->GetName();
		info.defaults_to_zero = FeatureDefaultsToZero(*expr);
		expr->SetAlias(info.internal_alias);
		feature_columns.push_back(std::move(info));
	}
	if (feature_columns.empty()) {
		throw BinderException("CREATE FEATURE query must project at least one feature expression");
	}

	inner_node.where_clause = FeatureConjoin(std::move(inner_node.where_clause), FeatureWindowFilter(parameters));

	auto inner_statement = make_uniq<SelectStatement>();
	inner_statement->node = std::move(inner_node_base);
	auto agg_ref = make_uniq<SubqueryRef>(std::move(inner_statement), "agg");

	auto result_select = make_uniq<SelectNode>();

	// Entity key columns come from the entity table so that entities without events still produce a row.
	for (idx_t i = 0; i < parameters.entity_columns.size(); i++) {
		auto &key_column = parameters.entity_key_columns[i];
		auto entity_ref = FeatureColumnRef("entity", key_column);
		entity_ref->SetAlias(parameters.entity_columns[i]);
		result_select->select_list.push_back(std::move(entity_ref));
	}

	for (auto &feature_column : feature_columns) {
		unique_ptr<ParsedExpression> projection = FeatureColumnRef("agg", feature_column.internal_alias);
		if (feature_column.defaults_to_zero) {
			projection = make_uniq<OperatorExpression>(ExpressionType::OPERATOR_COALESCE, std::move(projection),
			                                           make_uniq<ConstantExpression>(Value::INTEGER(0)));
		}
		projection->SetAlias(feature_column.output_name);
		result_select->select_list.push_back(std::move(projection));
	}

	if (parameters.entity_columns.empty()) {
		// Global feature: no entity table, the aggregate subquery already yields a single row.
		result_select->from_table = std::move(agg_ref);
	} else {
		unique_ptr<ParsedExpression> join_condition;
		for (idx_t i = 0; i < parameters.entity_columns.size(); i++) {
			auto condition = make_uniq<ComparisonExpression>(
			    ExpressionType::COMPARE_EQUAL, FeatureColumnRef("entity", parameters.entity_key_columns[i]),
			    FeatureColumnRef("agg", parameters.entity_columns[i]));
			join_condition = FeatureConjoin(std::move(join_condition), std::move(condition));
		}

		auto join = make_uniq<JoinRef>(JoinRefType::REGULAR);
		join->type = JoinType::LEFT;
		join->left = FeatureDistinctEntitySource(parameters.entity_table, parameters.entity_key_columns);
		join->right = std::move(agg_ref);
		join->condition = std::move(join_condition);
		result_select->from_table = std::move(join);
	}

	auto result = make_uniq<SelectStatement>();
	result->node = std::move(result_select);
	return result;
}

} // namespace duckdb
