#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {
using namespace std;

BoundAggregateExpression::BoundAggregateExpression(AggregateFunction function, vector<unique_ptr<Expression>> children, unique_ptr<FunctionData> bind_info, bool distinct)
	: Expression(ExpressionType::BOUND_AGGREGATE, ExpressionClass::BOUND_AGGREGATE, function.return_type),
		function(move(function)), children(move(children)), bind_info(move(bind_info)), distinct(distinct) {
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
	vector<unique_ptr<Expression>> new_children;
	for (auto &child : children) {
		new_children.push_back(child->Copy());
	}
	auto new_bind_info = bind_info->Copy();
	auto copy = make_unique<BoundAggregateExpression>(function, move(new_children), move(new_bind_info), distinct);
	copy->CopyProperties(*this);
	return move(copy);
}

} // namespace duckdb
