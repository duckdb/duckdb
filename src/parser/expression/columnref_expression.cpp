#include "parser/expression/columnref_expression.hpp"

#include "common/exception.hpp"
#include "common/serializer.hpp"
#include "common/types/hash.hpp"

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
	copy->depth = depth;
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
	serializer.Write<size_t>(depth);
}

unique_ptr<Expression> ColumnRefExpression::Deserialize(ExpressionType type, TypeId return_type, Deserializer &source) {
	auto table_name = source.Read<string>();
	auto column_name = source.Read<string>();
	auto index = source.Read<size_t>();
	auto depth = source.Read<size_t>();

	auto expression = make_unique<ColumnRefExpression>(column_name, table_name);
	expression->index = index;
	expression->depth = depth;
	return expression;
}

void ColumnRefExpression::ResolveType() {
	Expression::ResolveType();
	if (return_type == TypeId::INVALID) {
		throw Exception("Type of ColumnRefExpression was not resolved!");
	}
}

bool ColumnRefExpression::Equals(const Expression *other_) const {
	if (!Expression::Equals(other_)) {
		return false;
	}
	auto other = (ColumnRefExpression *)other_;
	if (index != (size_t) -1) {
		return index == other->index;
	} else {
		return column_name == other->column_name && table_name == other->table_name;
	}
}

uint64_t ColumnRefExpression::Hash() const {
	uint64_t result = Expression::Hash();
	result = CombineHash(result, duckdb::Hash<const char *>(column_name.c_str()));
	result = CombineHash(result, duckdb::Hash<const char *>(table_name.c_str()));
	return result;
}

string ColumnRefExpression::ToString() const {
	if (column_name.empty() && index != (size_t)-1) {
		return "#" + std::to_string(index);
	}
	return column_name.empty() ? std::to_string(binding.column_index) : column_name;
}
