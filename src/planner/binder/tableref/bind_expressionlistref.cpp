#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/tableref/expressionlistref.hpp"
#include "duckdb/planner/expression_binder/insert_binder.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/operator/logical_expression_get.hpp"
#include "duckdb/planner/operator/logical_dummy_scan.hpp"

namespace duckdb {

BoundStatement Binder::Bind(ExpressionListRef &expr) {
	BoundStatement result;
	result.types = expr.expected_types;
	result.names = expr.expected_names;

	vector<vector<unique_ptr<Expression>>> values;
	auto prev_can_contain_nulls = this->can_contain_nulls;
	// bind value list
	InsertBinder binder(*this, context);
	binder.target_type = LogicalType(LogicalTypeId::INVALID);
	for (idx_t list_idx = 0; list_idx < expr.values.size(); list_idx++) {
		auto &expression_list = expr.values[list_idx];
		if (result.names.empty()) {
			// no names provided, generate them
			for (idx_t val_idx = 0; val_idx < expression_list.size(); val_idx++) {
				result.names.push_back("col" + to_string(val_idx));
			}
		}

		this->can_contain_nulls = true;
		vector<unique_ptr<Expression>> list;
		for (idx_t val_idx = 0; val_idx < expression_list.size(); val_idx++) {
			if (!result.types.empty()) {
				D_ASSERT(result.types.size() == expression_list.size());
				binder.target_type = result.types[val_idx];
			}
			auto bound_expr = binder.Bind(expression_list[val_idx]);
			list.push_back(std::move(bound_expr));
		}
		values.push_back(std::move(list));
		this->can_contain_nulls = prev_can_contain_nulls;
	}
	if (result.types.empty() && !expr.values.empty()) {
		// there are no types specified
		// we have to figure out the result types
		// for each column, we iterate over all of the expressions and select the max logical type
		// we initialize all types to SQLNULL
		result.types.resize(expr.values[0].size(), LogicalType::SQLNULL);
		// now loop over the lists and select the max logical type
		for (idx_t list_idx = 0; list_idx < values.size(); list_idx++) {
			auto &list = values[list_idx];
			for (idx_t val_idx = 0; val_idx < list.size(); val_idx++) {
				auto &current_type = result.types[val_idx];
				auto next_type = ExpressionBinder::GetExpressionReturnType(*list[val_idx]);
				result.types[val_idx] = LogicalType::MaxLogicalType(context, current_type, next_type);
			}
		}
		for (auto &type : result.types) {
			type = LogicalType::NormalizeType(type);
		}
		// finally do another loop over the expressions and add casts where required
		for (idx_t list_idx = 0; list_idx < values.size(); list_idx++) {
			auto &list = values[list_idx];
			for (idx_t val_idx = 0; val_idx < list.size(); val_idx++) {
				list[val_idx] =
				    BoundCastExpression::AddCastToType(context, std::move(list[val_idx]), result.types[val_idx]);
			}
		}
	}
	auto bind_index = GenerateTableIndex();
	bind_context.AddGenericBinding(bind_index, expr.alias, result.names, result.types);

	// values list, first plan any subqueries in the list
	auto root = make_uniq_base<LogicalOperator, LogicalDummyScan>(GenerateTableIndex());
	for (auto &expr_list : values) {
		for (auto &expr : expr_list) {
			PlanSubqueries(expr, root);
		}
	}

	auto expr_get = make_uniq<LogicalExpressionGet>(bind_index, result.types, std::move(values));
	expr_get->AddChild(std::move(root));
	result.plan = std::move(expr_get);
	return result;
}

} // namespace duckdb
