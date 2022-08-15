#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"

#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"

namespace duckdb {

BoundAggregateExpression::BoundAggregateExpression(AggregateFunction function, vector<unique_ptr<Expression>> children,
                                                   unique_ptr<Expression> filter, unique_ptr<FunctionData> bind_info,
                                                   bool distinct)
    : Expression(ExpressionType::BOUND_AGGREGATE, ExpressionClass::BOUND_AGGREGATE, function.return_type),
      function(move(function)), children(move(children)), bind_info(move(bind_info)), distinct(distinct),
      filter(move(filter)) {
	D_ASSERT(!function.name.empty());
}

string BoundAggregateExpression::ToString() const {
	return FunctionExpression::ToString<BoundAggregateExpression, Expression>(*this, string(), function.name, false,
	                                                                          distinct, filter.get());
}

hash_t BoundAggregateExpression::Hash() const {
	hash_t result = Expression::Hash();
	result = CombineHash(result, function.Hash());
	result = CombineHash(result, duckdb::Hash(distinct));
	return result;
}

bool BoundAggregateExpression::Equals(const BaseExpression *other_p) const {
	if (!Expression::Equals(other_p)) {
		return false;
	}
	auto other = (BoundAggregateExpression *)other_p;
	if (other->distinct != distinct) {
		return false;
	}
	if (other->function != function) {
		return false;
	}
	if (children.size() != other->children.size()) {
		return false;
	}
	if (!Expression::Equals(other->filter.get(), filter.get())) {
		return false;
	}
	for (idx_t i = 0; i < children.size(); i++) {
		if (!Expression::Equals(children[i].get(), other->children[i].get())) {
			return false;
		}
	}
	if (!FunctionData::Equals(bind_info.get(), other->bind_info.get())) {
		return false;
	}
	return true;
}

bool BoundAggregateExpression::PropagatesNullValues() const {
	return function.null_handling == FunctionNullHandling::SPECIAL_HANDLING ? false
	                                                                        : Expression::PropagatesNullValues();
}

unique_ptr<Expression> BoundAggregateExpression::Copy() {
	vector<unique_ptr<Expression>> new_children;
	for (auto &child : children) {
		new_children.push_back(child->Copy());
	}
	auto new_bind_info = bind_info ? bind_info->Copy() : nullptr;
	auto new_filter = filter ? filter->Copy() : nullptr;
	auto copy = make_unique<BoundAggregateExpression>(function, move(new_children), move(new_filter),
	                                                  move(new_bind_info), distinct);
	copy->CopyProperties(*this);
	return move(copy);
}

} // namespace duckdb
