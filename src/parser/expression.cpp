
#include "common/serializer.hpp"

#include "parser/expression.hpp"

using namespace duckdb;
using namespace std;

bool Expression::IsAggregate() {
	bool is_aggregate = false;
	for (auto &child : children) {
		is_aggregate |= child->IsAggregate();
	}
	return is_aggregate;
}

bool Expression::IsScalar() {
	bool is_scalar = true;
	for (auto &child : children) {
		is_scalar &= child->IsScalar();
	}
	return is_scalar;
}

void Expression::GetAggregates(
    std::vector<AggregateExpression *> &expressions) {
	for (auto &child : children) {
		child->GetAggregates(expressions);
	}
}

bool Expression::HasSubquery() {
	for (auto &child : children) {
		if (child->HasSubquery()) {
			return true;
		}
	}
	return false;
}

void Expression::Serialize(Serializer &serializer) {
	serializer.Write<int>((int)type);
	serializer.Write<int>((int)return_type);
	serializer.Write<uint32_t>(children.size());
	for (auto &children : children) {
		children->Serialize(serializer);
	}
}

unique_ptr<Expression> Expression::Deserialize(Deserializer &source) {
	bool failed = false;
	auto type = (ExpressionType)source.Read<int>(failed);
	auto return_type = (TypeId)source.Read<int>(failed);
	auto children_count = source.Read<uint32_t>(failed);
	if (failed) {
		return nullptr;
	}
	// deserialize the children
	vector<unique_ptr<Expression>> expressions;
	for (size_t i = 0; i < children_count; i++) {
		auto expression = Expression::Deserialize(source);
		if (!expression) {
			return nullptr;
		}
		expressions.push_back(move(expression));
	}
	switch (type) {
	default:
		return nullptr;
	}
}
