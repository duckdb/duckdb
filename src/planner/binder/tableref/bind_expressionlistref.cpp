#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/tableref/bound_expressionlistref.hpp"
#include "duckdb/parser/tableref/expressionlistref.hpp"
#include "duckdb/planner/expression_binder/insert_binder.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"

namespace duckdb {

unique_ptr<BoundTableRef> Binder::Bind(ExpressionListRef &expr) {
	auto result = make_uniq<BoundExpressionListRef>();
	result->types = expr.expected_types;
	result->names = expr.expected_names;
	// bind value list
	InsertBinder binder(*this, context);
	binder.target_type = LogicalType(LogicalTypeId::INVALID);
	for (idx_t list_idx = 0; list_idx < expr.values.size(); list_idx++) {
		auto &expression_list = expr.values[list_idx];
		if (result->names.empty()) {
			// no names provided, generate them
			for (idx_t val_idx = 0; val_idx < expression_list.size(); val_idx++) {
				result->names.push_back("col" + to_string(val_idx));
			}
		}

		vector<unique_ptr<Expression>> list;
		for (idx_t val_idx = 0; val_idx < expression_list.size(); val_idx++) {
			if (!result->types.empty()) {
				D_ASSERT(result->types.size() == expression_list.size());
				binder.target_type = result->types[val_idx];
			}
			auto expr = binder.Bind(expression_list[val_idx]);
			list.push_back(std::move(expr));
		}
		result->values.push_back(std::move(list));
	}
	if (result->types.empty() && !expr.values.empty()) {
		// there are no types specified
		// we have to figure out the result types
		// for each column, we iterate over all of the expressions and select the max logical type
		// we initialize all types to SQLNULL
		result->types.resize(expr.values[0].size(), LogicalType::SQLNULL);
		// now loop over the lists and select the max logical type
		for (idx_t list_idx = 0; list_idx < result->values.size(); list_idx++) {
			auto &list = result->values[list_idx];
			for (idx_t val_idx = 0; val_idx < list.size(); val_idx++) {
				result->types[val_idx] =
				    LogicalType::MaxLogicalType(result->types[val_idx], list[val_idx]->return_type);
			}
		}
		// finally do another loop over the expressions and add casts where required
		for (idx_t list_idx = 0; list_idx < result->values.size(); list_idx++) {
			auto &list = result->values[list_idx];
			for (idx_t val_idx = 0; val_idx < list.size(); val_idx++) {
				list[val_idx] =
				    BoundCastExpression::AddCastToType(context, std::move(list[val_idx]), result->types[val_idx]);
			}
		}
	}
	result->bind_index = GenerateTableIndex();
	bind_context.AddGenericBinding(result->bind_index, expr.alias, result->names, result->types);
	return std::move(result);
}

} // namespace duckdb
