#include "parquet_shredding.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/type_visitor.hpp"

namespace duckdb {

ChildShreddingTypes::ChildShreddingTypes() : types(make_uniq<case_insensitive_map_t<ShreddingType>>()) {
}

ChildShreddingTypes ChildShreddingTypes::Copy() const {
	ChildShreddingTypes result;
	for (const auto &type : *types) {
		result.types->emplace(type.first, type.second.Copy());
	}
	return result;
}

ShreddingType::ShreddingType() : set(false) {
}

ShreddingType::ShreddingType(const LogicalType &type) : set(true), type(type) {
}

ShreddingType ShreddingType::Copy() const {
	auto result = set ? ShreddingType(type) : ShreddingType();
	result.children = children.Copy();
	return result;
}

static ShreddingType ConvertShreddingTypeRecursive(const LogicalType &type) {
	if (type.id() == LogicalTypeId::VARIANT) {
		return ShreddingType(LogicalType(LogicalTypeId::ANY));
	}
	if (!type.IsNested()) {
		return ShreddingType(type);
	}

	switch (type.id()) {
	case LogicalTypeId::STRUCT: {
		ShreddingType res(type);
		auto &children = StructType::GetChildTypes(type);
		for (auto &entry : children) {
			res.AddChild(entry.first, ConvertShreddingTypeRecursive(entry.second));
		}
		return res;
	}
	case LogicalTypeId::LIST: {
		ShreddingType res(type);
		const auto &child = ListType::GetChildType(type);
		res.AddChild("element", ConvertShreddingTypeRecursive(child));
		return res;
	}
	default:
		break;
	}
	throw BinderException("VARIANT can only be shredded on LIST/STRUCT/ANY/non-nested type, not %s", type.ToString());
}

void ShreddingType::AddChild(const string &name, ShreddingType &&child) {
	children.types->emplace(name, std::move(child));
}

optional_ptr<const ShreddingType> ShreddingType::GetChild(const string &name) const {
	auto it = children.types->find(name);
	if (it == children.types->end()) {
		return nullptr;
	}
	return it->second;
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
