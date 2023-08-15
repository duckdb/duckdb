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
	D_ASSERT(!this->function.name.empty());
}

string BoundAggregateExpression::ToString() const {
	return FunctionExpression::ToString<BoundAggregateExpression, Expression, BoundOrderModifier>(
	    *this, string(), function.name, false, IsDistinct(), filter.get(), order_bys.get());
}

hash_t BoundAggregateExpression::Hash() const {
	hash_t result = Expression::Hash();
	result = CombineHash(result, function.Hash());
	result = CombineHash(result, duckdb::Hash(IsDistinct()));
	return result;
}

bool BoundAggregateExpression::Equals(const BaseExpression &other_p) const {
	if (!Expression::Equals(other_p)) {
		return false;
	}
	auto &other = other_p.Cast<BoundAggregateExpression>();
	if (other.aggr_type != aggr_type) {
		return false;
	}
	if (other.function != function) {
		return false;
	}
	if (children.size() != other.children.size()) {
		return false;
	}
	if (!Expression::Equals(other.filter, filter)) {
		return false;
	}
	for (idx_t i = 0; i < children.size(); i++) {
		if (!Expression::Equals(*children[i], *other.children[i])) {
			return false;
		}
	}
	if (!FunctionData::Equals(bind_info.get(), other.bind_info.get())) {
		return false;
	}
	if (!BoundOrderModifier::Equals(order_bys, other.order_bys)) {
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
	auto copy = make_uniq<BoundAggregateExpression>(function, std::move(new_children), std::move(new_filter),
	                                                std::move(new_bind_info), aggr_type);
	copy->CopyProperties(*this);
	copy->order_bys = order_bys ? order_bys->Copy() : nullptr;
	return std::move(copy);
}

void BoundAggregateExpression::Serialize(FieldWriter &writer) const {
	writer.WriteField(IsDistinct());
	writer.WriteOptional(filter);
	writer.WriteOptional(order_bys);
	FunctionSerializer::Serialize<AggregateFunction>(writer, function, return_type, children, bind_info.get());
}

unique_ptr<Expression> BoundAggregateExpression::Deserialize(ExpressionDeserializationState &state,
                                                             FieldReader &reader) {
	auto distinct = reader.ReadRequired<bool>();
	auto filter = reader.ReadOptional<Expression>(nullptr, state.gstate);
	auto order_bys = reader.ReadOptional<BoundOrderModifier>(nullptr, state.gstate);
	vector<unique_ptr<Expression>> children;
	unique_ptr<FunctionData> bind_info;
	auto function = FunctionSerializer::Deserialize<AggregateFunction, AggregateFunctionCatalogEntry>(
	    reader, state, CatalogType::AGGREGATE_FUNCTION_ENTRY, children, bind_info);

	auto x = make_uniq<BoundAggregateExpression>(function, std::move(children), std::move(filter), std::move(bind_info),
	                                             distinct ? AggregateType::DISTINCT : AggregateType::NON_DISTINCT);
	x->order_bys = std::move(order_bys);
	return std::move(x);
}

void BoundAggregateExpression::FormatSerialize(FormatSerializer &serializer) const {
	Expression::FormatSerialize(serializer);
	serializer.WriteProperty(200, "return_type", return_type);
	serializer.WriteProperty(201, "children", children);
	FunctionSerializer::FormatSerialize(serializer, function, bind_info.get());
	serializer.WriteProperty(203, "aggregate_type", aggr_type);
	serializer.WriteOptionalProperty(204, "filter", filter);
	serializer.WriteOptionalProperty(205, "order_bys", order_bys);
}

unique_ptr<Expression> BoundAggregateExpression::FormatDeserialize(FormatDeserializer &deserializer) {
	auto return_type = deserializer.ReadProperty<LogicalType>(200, "return_type");
	auto children = deserializer.ReadProperty<vector<unique_ptr<Expression>>>(201, "children");
	auto entry = FunctionSerializer::FormatDeserialize<AggregateFunction, AggregateFunctionCatalogEntry>(
	    deserializer, CatalogType::AGGREGATE_FUNCTION_ENTRY, children);
	auto aggregate_type = deserializer.ReadProperty<AggregateType>(203, "aggregate_type");
	auto filter = deserializer.ReadOptionalProperty<unique_ptr<Expression>>(204, "filter");
	auto result = make_uniq<BoundAggregateExpression>(std::move(entry.first), std::move(children), std::move(filter),
	                                                  std::move(entry.second), aggregate_type);
	deserializer.ReadOptionalProperty(205, "order_bys", result->order_bys);
	return std::move(result);
}

} // namespace duckdb
