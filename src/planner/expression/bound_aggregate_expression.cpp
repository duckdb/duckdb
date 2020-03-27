#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/string_util.hpp"

using namespace duckdb;
using namespace std;

BoundAggregateExpression::BoundAggregateExpression(TypeId return_type, AggregateFunction function, bool distinct)
    : Expression(ExpressionType::BOUND_AGGREGATE, ExpressionClass::BOUND_AGGREGATE, return_type), function(function),
      distinct(distinct) {
}

string BoundAggregateExpression::ToString() const {
	string result = function.name + "(";
	if (distinct) {
		result += "DISTINCT ";
	}
	StringUtil::Join(children, children.size(), ", ",
	                 [](const unique_ptr<Expression> &child) { return child->GetName(); });
	result += ")";
	return result;
}
hash_t BoundAggregateExpression::Hash() const {
	hash_t result = Expression::Hash();
	result = CombineHash(result, duckdb::Hash(function.name.c_str()));
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
	if (other->function != function) {
		return false;
	}
	if (children.size() != other->children.size()) {
		return false;
	}
	for (idx_t i = 0; i < children.size(); i++) {
		if (!Expression::Equals(children[i].get(), other->children[i].get())) {
			return false;
		}
	}
	return true;
}

unique_ptr<Expression> BoundAggregateExpression::Copy() {
	auto copy = make_unique<BoundAggregateExpression>(return_type, function, distinct);
	for (auto &child : children) {
		copy->children.push_back(child->Copy());
	}
	copy->CopyProperties(*this);
	return move(copy);
}
