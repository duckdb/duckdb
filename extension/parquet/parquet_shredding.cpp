#include "parquet_shredding.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/type_visitor.hpp"

namespace duckdb {

ShreddingType::ShreddingType() : set(false) {
}

ShreddingType::ShreddingType(LogicalTypeId shredding_type) : set(true), shredding_type(shredding_type) {
}

static ShreddingType ConvertShreddingTypeRecursive(const LogicalType &type) {
	if (type.id() == LogicalTypeId::ANY) {
		return ShreddingType(type.id());
	}
	if (!type.IsNested()) {
		return ShreddingType(type.id());
	}

	switch (type.id()) {
	case LogicalTypeId::STRUCT: {
		ShreddingType res(LogicalTypeId::STRUCT);
		auto &children = StructType::GetChildTypes(type);
		for (auto &entry : children) {
			res.children[entry.first] = ConvertShreddingTypeRecursive(entry.second);
		}
		return res;
	}
	case LogicalTypeId::LIST: {
		ShreddingType res(LogicalTypeId::STRUCT);
		const auto &child = ListType::GetChildType(type);
		res.children["element"] = ConvertShreddingTypeRecursive(child);
		return res;
	}
	default:
		break;
	}
	throw BinderException("VARIANT can only be shredded on LIST/STRUCT/ANY/non-nested type, not %s", type.ToString());
}

ShreddingType ShreddingType::GetShreddingTypes(const Value &val) {
	if (val.type().id() != LogicalTypeId::VARCHAR) {
		throw BinderException("SHREDDING value should be of type VARCHAR, a stringified type to use for the column");
	}
	auto type_str = val.GetValue<string>();
	auto logical_type = TransformStringToLogicalType(type_str);

	return ConvertShreddingTypeRecursive(logical_type);
}

} // namespace duckdb
