#include "planner/expression/bound_aggregate_expression.hpp"
#include "catalog/catalog_entry/aggregate_function_catalog_entry.hpp"

using namespace duckdb;
using namespace std;

BoundAggregateExpression::BoundAggregateExpression(TypeId return_type, unique_ptr<Expression> child,
                                                   AggregateFunctionCatalogEntry *bound_aggregate, bool distinct)
    : Expression(ExpressionType::BOUND_AGGREGATE, ExpressionClass::BOUND_AGGREGATE, return_type),
      bound_aggregate(bound_aggregate), distinct(distinct), child(move(child)) {
}

string BoundAggregateExpression::ToString() const {
	return bound_aggregate->name + "(" + (distinct ? "DISTINCT " : " ") + (child ? child->GetName() : string()) + ")";
}

uint64_t BoundAggregateExpression::Hash() const {
	uint64_t result = Expression::Hash();
	result = CombineHash(result, duckdb::Hash(bound_aggregate->name.c_str()));
	result = CombineHash(result, duckdb::Hash(distinct));
	return result;
}

bool BoundAggregateExpression::Equals(const BaseExpression *other_) const {
	if (!BaseExpression::Equals(other_)) {
		return false;
	}
	auto other = (BoundAggregateExpression *)other_;
	if (other->distinct != distinct) {
		return false;
	}
	if (other->bound_aggregate != bound_aggregate) {
		return false;
	}
	return Expression::Equals(child.get(), other->child.get());
}

unique_ptr<Expression> BoundAggregateExpression::Copy() {
	auto new_child = child ? child->Copy() : nullptr;
	auto new_aggregate = make_unique<BoundAggregateExpression>(return_type, move(new_child), bound_aggregate, distinct);
	new_aggregate->CopyProperties(*this);
	return move(new_aggregate);
}
