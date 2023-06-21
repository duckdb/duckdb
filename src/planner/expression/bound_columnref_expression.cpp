#include "duckdb/planner/expression/bound_columnref_expression.hpp"

#include "duckdb/common/field_writer.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

BoundColumnRefExpression::BoundColumnRefExpression(string alias_p, LogicalType type, ColumnBinding binding, idx_t depth)
    : Expression(ExpressionType::BOUND_COLUMN_REF, ExpressionClass::BOUND_COLUMN_REF, std::move(type)),
      binding(binding), depth(depth) {
	this->alias = std::move(alias_p);
}

BoundColumnRefExpression::BoundColumnRefExpression(LogicalType type, ColumnBinding binding, idx_t depth)
    : BoundColumnRefExpression(string(), std::move(type), binding, depth) {
}

unique_ptr<Expression> BoundColumnRefExpression::Copy() {
	return make_uniq<BoundColumnRefExpression>(alias, return_type, binding, depth);
}

hash_t BoundColumnRefExpression::Hash() const {
	auto result = Expression::Hash();
	result = CombineHash(result, duckdb::Hash<uint64_t>(binding.column_index));
	result = CombineHash(result, duckdb::Hash<uint64_t>(binding.table_index));
	return CombineHash(result, duckdb::Hash<uint64_t>(depth));
}

bool BoundColumnRefExpression::Equals(const BaseExpression &other_p) const {
	if (!Expression::Equals(other_p)) {
		return false;
	}
	auto &other = other_p.Cast<BoundColumnRefExpression>();
	return other.binding == binding && other.depth == depth;
}

string BoundColumnRefExpression::GetName() const {
#ifdef DEBUG
	if (DBConfigOptions::debug_print_bindings) {
		return binding.ToString();
	}
#endif
	return Expression::GetName();
}

string BoundColumnRefExpression::ToString() const {
#ifdef DEBUG
	if (DBConfigOptions::debug_print_bindings) {
		return binding.ToString();
	}
#endif
	if (!alias.empty()) {
		return alias;
	}
	return binding.ToString();
}

void BoundColumnRefExpression::Serialize(FieldWriter &writer) const {
	writer.WriteString(alias);
	writer.WriteSerializable(return_type);
	writer.WriteField(binding.table_index);
	writer.WriteField(binding.column_index);
	writer.WriteField(depth);
}

unique_ptr<Expression> BoundColumnRefExpression::Deserialize(ExpressionDeserializationState &state,
                                                             FieldReader &reader) {
	auto alias = reader.ReadRequired<string>();
	auto return_type = reader.ReadRequiredSerializable<LogicalType, LogicalType>();
	auto table_index = reader.ReadRequired<idx_t>();
	auto column_index = reader.ReadRequired<idx_t>();
	auto depth = reader.ReadRequired<idx_t>();

	return make_uniq<BoundColumnRefExpression>(alias, return_type, ColumnBinding(table_index, column_index), depth);
}

} // namespace duckdb
