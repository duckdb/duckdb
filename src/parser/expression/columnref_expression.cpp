#include "parser/expression/columnref_expression.hpp"

#include "common/exception.hpp"
#include "common/serializer.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<Expression> ColumnRefExpression::Copy() {
	// should not make a copy with a reference because reference is not owned
	// we cannot make a copy of reference along with it -> might result in
	// original reference being freed
	assert(!reference);

	auto copy = make_unique<ColumnRefExpression>(column_name, table_name);
	copy->CopyProperties(*this);
	copy->binding = binding;
	copy->index = index;
	copy->reference = reference;
	return copy;
}

void ColumnRefExpression::Serialize(Serializer &serializer) {
	Expression::Serialize(serializer);
	assert(!reference);
	serializer.WriteString(table_name);
	serializer.WriteString(column_name);
	serializer.Write<size_t>(index);
}

unique_ptr<Expression> ColumnRefExpression::Deserialize(ExpressionDeserializeInfo *info, Deserializer &source) {
	auto table_name = source.Read<string>();
	auto column_name = source.Read<string>();
	auto index = source.Read<size_t>();

	if (info->children.size() > 0) {
		throw SerializationException("ColumnRef cannot have children!");
	}

	auto expression = make_unique<ColumnRefExpression>(column_name, table_name);
	expression->index = index;
	return expression;
}

void ColumnRefExpression::ResolveType() {
	Expression::ResolveType();
	if (return_type == TypeId::INVALID) {
		throw Exception("Type of ColumnRefExpression was not resolved!");
	}
}

bool ColumnRefExpression::Equals(const Expression *other_) {
	if (!Expression::Equals(other_)) {
		return false;
	}
	auto other = reinterpret_cast<const ColumnRefExpression *>(other_);
	if (!other) {
		return false;
	}
	return column_name == other->column_name && table_name == other->table_name;
}

string ColumnRefExpression::ToString() const {
	if (column_name.empty() && index != (size_t)-1) {
		return "#" + std::to_string(index);
	}
	return column_name.empty() ? std::to_string(binding.column_index) : column_name;
}
