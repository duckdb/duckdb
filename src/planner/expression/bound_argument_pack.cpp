#include "duckdb/planner/expression/bound_argument_pack.hpp"

namespace duckdb {

bool ArgumentPack::IsPackType(const LogicalType &type) {
	return StructType::IsStruct(type) && type.GetAlias() == TYPE_ALIAS;
}

LogicalType ArgumentPack::PositionalType(vector<LogicalType> element_types) {
	return LogicalType::TUPLE(std::move(element_types)).WithAlias(TYPE_ALIAS);
}

LogicalType ArgumentPack::KeywordType(child_list_t<LogicalType> value_types) {
	return LogicalType::STRUCT(std::move(value_types)).WithAlias(TYPE_ALIAS);
}

unique_ptr<Expression> ArgumentPack::Create(vector<unique_ptr<Expression>> children, LogicalType pack_type) {
	D_ASSERT(IsPackType(pack_type));
	D_ASSERT(StructType::GetChildCount(pack_type) == children.size());
	auto result = make_uniq<BoundOperatorExpression>(ExpressionType::ARGUMENT_PACK, std::move(pack_type));
	result->GetChildrenMutable() = std::move(children);
	return std::move(result);
}

} // namespace duckdb
