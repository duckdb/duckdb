#include "planner/expression/bound_aggregate_expression.hpp"
#include "catalog/catalog_entry/aggregate_function_catalog_entry.hpp"

using namespace duckdb;
using namespace std;

BoundAggregateExpression::BoundAggregateExpression(TypeId return_type, AggregateFunctionCatalogEntry *bound_aggregate,
                                                   bool distinct)
    : Expression(ExpressionType::BOUND_AGGREGATE, ExpressionClass::BOUND_AGGREGATE, return_type),
      bound_aggregate(bound_aggregate), distinct(distinct) {
}

string BoundAggregateExpression::ToString() const {
	string str = bound_aggregate->name + "(";
	if (distinct) {
		str += "DISTINCT ";
	}
	for (index_t i = 0; i < children.size(); i++) {
		if (i > 0) {
			str += ", ";
		}
		str += children[i]->GetName();
	}
	str += ")";
	return str;
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
	if (children.size() != other->children.size()) {
		return false;
	}
	for (index_t i = 0; i < children.size(); i++) {
		if (!Expression::Equals(children[i].get(), other->children[i].get())) {
			return false;
		}
	}
	return true;
}

unique_ptr<Expression> BoundAggregateExpression::Copy() {
	auto copy = make_unique<BoundAggregateExpression>(return_type, bound_aggregate, distinct);
	for (auto &child : children) {
		copy->children.push_back(child->Copy());
	}
	copy->CopyProperties(*this);
	return move(copy);
}
