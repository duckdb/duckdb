
#include "parser/expression/columnref_expression.hpp"

#include "common/exception.hpp"
#include "common/serializer.hpp"

using namespace duckdb;
using namespace std;

void ColumnRefExpression::Serialize(Serializer &serializer) {
	Expression::Serialize(serializer);
	assert(type == ExpressionType::COLUMN_REF);
	assert(!reference);
	assert(!column_name.empty());
	serializer.WriteString(table_name);
	serializer.WriteString(column_name);
	serializer.Write<size_t>(index);
}

unique_ptr<Expression>
ColumnRefExpression::Deserialize(ExpressionDeserializeInformation *info,
                                 Deserializer &source) {
	bool failed = false;
	auto table_name = source.Read<string>(failed);
	auto column_name = source.Read<string>(failed);
	auto index = source.Read<size_t>(failed);
	if (failed) {
		return nullptr;
	}

	auto expression = make_unique<ColumnRefExpression>(column_name, table_name);
	expression->index = index;
	expression->children = move(info->children);
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
	if (index != (size_t)-1) {
		return "#" + std::to_string(index);
	}
	auto str =
	    table_name.empty() ? std::to_string(binding.table_index) : table_name;
	str += ".";
	str += column_name.empty() ? std::to_string(binding.column_index)
	                           : column_name;
	return str;
}
