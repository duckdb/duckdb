#include "duckdb/planner/expression_binder/column_alias_binder.hpp"

#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

// alias map will be modified during initial select column list binding.
ColumnAliasLookup::ColumnAliasLookup(const case_insensitive_map_t<idx_t> &alias_map) : alias_map(alias_map) {
}

idx_t ColumnAliasLookup::TryBindAlias(const ParsedExpression &expr) {
	if (expr.expression_class != ExpressionClass::COLUMN_REF) {
		return DConstants::INVALID_INDEX;
	}
	auto &colref = (ColumnRefExpression &)expr;

	if (colref.IsQualified()) {
		return DConstants::INVALID_INDEX;
	}
	auto alias_name = colref.column_names[0];

	auto index = alias_map.find(alias_name);
	if (index == alias_map.end()) {
		return DConstants::INVALID_INDEX;
	};

	// TODO, this is for order by
	if (index->second == DConstants::INVALID_INDEX) {
		throw BinderException(
		    StringUtil::Format("Tried to reference alias \"%s\", which has been poisoned.", alias_name));
	}

	return index->second;
}

ColumnAliasBinder::ColumnAliasBinder(const BoundSelectNode &node) : node(node) {
}

unique_ptr<ParsedExpression> ColumnAliasBinder::ResolveAliasByDuplicatingParsedTarget(idx_t index) {
	auto select_length = node.original_expressions.size();
	if (index >= select_length) {
		throw InternalException(StringUtil::Format("Tried to duplicate index %lld of a select list with %lld items.",
		                                           index, select_length));
	}
	auto resolved_select_item = node.original_expressions[index]->Copy();
	return resolved_select_item;
}

BindResult ColumnAliasBinder::BindAliasByDuplicatingParsedTarget(ExpressionBinder *binder, const ParsedExpression &expr,
                                                                 idx_t index, idx_t depth, bool root_expression) {
	auto resolved_select_item = ResolveAliasByDuplicatingParsedTarget(index);
	auto result = binder->BindExpression(&resolved_select_item, depth, root_expression);
	if (!result.HasError() && result.expression->HasSideEffects()) {
		throw BinderException(
		    StringUtil::Format("Alias \"%s\" tried to reference \"%s\", but this has side effects so is not allowed.",
		                       expr.alias, resolved_select_item->ToString()));
	}
	return result;
}

ColumnAliasProjectionBinder::ColumnAliasProjectionBinder(idx_t projection_idx) : projection_index(projection_idx) {
}

unique_ptr<Expression> ColumnAliasProjectionBinder::ResolveAliasWithProjection(const ParsedExpression &expr,
                                                                               idx_t index) {
	if (projection_index == DConstants::INVALID_INDEX) {
		throw InternalException(StringUtil::Format(
		    "Tried to bind %s with a projection to this query, but projection_index has not yet been set.",
		    expr.ToString()));
	}
	string alias = expr.GetName();
	return make_unique<BoundColumnRefExpression>(move(alias), LogicalType::INVALID,
	                                             ColumnBinding(projection_index, index));
}

} // namespace duckdb
