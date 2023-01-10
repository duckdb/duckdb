#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"

#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/function/function_serialization.hpp"

namespace duckdb {

BoundAggregateExpression::BoundAggregateExpression(AggregateFunction function, vector<unique_ptr<Expression>> children,
                                                   unique_ptr<Expression> filter, unique_ptr<FunctionData> bind_info,
                                                   AggregateType aggr_type)
    : Expression(ExpressionType::BOUND_AGGREGATE, ExpressionClass::BOUND_AGGREGATE, function.return_type),
      function(std::move(function)), children(std::move(children)), bind_info(std::move(bind_info)),
      aggr_type(aggr_type), filter(std::move(filter)) {
	D_ASSERT(!function.name.empty());
}

string BoundAggregateExpression::ToString() const {
	return FunctionExpression::ToString<BoundAggregateExpression, Expression>(*this, string(), function.name, false,
	                                                                          IsDistinct(), filter.get());
}

hash_t BoundAggregateExpression::Hash() const {
	hash_t result = Expression::Hash();
	result = CombineHash(result, function.Hash());
	result = CombineHash(result, duckdb::Hash(IsDistinct()));
	return result;
}

bool BoundAggregateExpression::Equals(const BaseExpression *other_p) const {
	if (!Expression::Equals(other_p)) {
		return false;
	}
	auto other = (BoundAggregateExpression *)other_p;
	if (other->aggr_type != aggr_type) {
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
	new_children.reserve(children.size());
	for (auto &child : children) {
		new_children.push_back(child->Copy());
	}
	auto new_bind_info = bind_info ? bind_info->Copy() : nullptr;
	auto new_filter = filter ? filter->Copy() : nullptr;
	auto copy = make_unique<BoundAggregateExpression>(function, std::move(new_children), std::move(new_filter),
	                                                  std::move(new_bind_info), aggr_type);
	copy->CopyProperties(*this);
	return std::move(copy);
}

void BoundAggregateExpression::Serialize(FieldWriter &writer) const {
	writer.WriteField(IsDistinct());
	writer.WriteOptional(filter);
	FunctionSerializer::Serialize<AggregateFunction>(writer, function, return_type, children, bind_info.get());
}

unique_ptr<Expression> BoundAggregateExpression::Deserialize(ExpressionDeserializationState &state,
                                                             FieldReader &reader) {
	auto distinct = reader.ReadRequired<bool>();
	auto filter = reader.ReadOptional<Expression>(nullptr, state.gstate);
	vector<unique_ptr<Expression>> children;
	unique_ptr<FunctionData> bind_info;
	auto function = FunctionSerializer::Deserialize<AggregateFunction, AggregateFunctionCatalogEntry>(
	    reader, state, CatalogType::AGGREGATE_FUNCTION_ENTRY, children, bind_info);

	return make_unique<BoundAggregateExpression>(function, std::move(children), std::move(filter), std::move(bind_info),
	                                             distinct ? AggregateType::DISTINCT : AggregateType::NON_DISTINCT);
}

} // namespace duckdb
