#include "parser/expression/columnref_expression.hpp"

#include "common/exception.hpp"
#include "common/serializer.hpp"
#include "common/types/hash.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<Expression> ColumnRefExpression::Copy() {
	auto copy = make_unique<ColumnRefExpression>(column_name, table_name);
	copy->CopyProperties(*this);
	return copy;
}

void ColumnRefExpression::Serialize(Serializer &serializer) {
	Expression::Serialize(serializer);
	serializer.WriteString(table_name);
	serializer.WriteString(column_name);
}

unique_ptr<Expression> ColumnRefExpression::Deserialize(ExpressionType type, TypeId return_type, Deserializer &source) {
	auto table_name = source.Read<string>();
	auto column_name = source.Read<string>();
	auto expression = make_unique<ColumnRefExpression>(column_name, table_name);
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
	return column_name == other->column_name && table_name == other->table_name;
}

uint64_t ColumnRefExpression::Hash() const {
	uint64_t result = Expression::Hash();
	result = CombineHash(result, duckdb::Hash<const char *>(column_name.c_str()));
	result = CombineHash(result, duckdb::Hash<const char *>(table_name.c_str()));
	return result;
}

string ColumnRefExpression::ToString() const {
	if (table_name.empty()) {
		return column_name;
	} else {
		return table_name + "." + column_name;
	}
}
