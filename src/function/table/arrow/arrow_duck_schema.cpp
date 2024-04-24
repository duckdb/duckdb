#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "duckdb/common/arrow/arrow.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

void ArrowTableType::AddColumn(idx_t index, unique_ptr<ArrowType> type) {
	D_ASSERT(arrow_convert_data.find(index) == arrow_convert_data.end());
	arrow_convert_data.emplace(std::make_pair(index, std::move(type)));
}

const arrow_column_map_t &ArrowTableType::GetColumns() const {
	return arrow_convert_data;
}

void ArrowType::SetDictionary(unique_ptr<ArrowType> dictionary) {
	D_ASSERT(!this->dictionary_type);
	dictionary_type = std::move(dictionary);
}

bool ArrowType::HasDictionary() const {
	return dictionary_type != nullptr;
}

const ArrowType &ArrowType::GetDictionary() const {
	D_ASSERT(dictionary_type);
	return *dictionary_type;
}

void ArrowType::SetRunEndEncoded() {
	D_ASSERT(type_info);
	D_ASSERT(type_info->type == ArrowTypeInfoType::STRUCT);
	auto &struct_info = type_info->Cast<ArrowStructInfo>();
	D_ASSERT(struct_info.ChildCount() == 2);

	auto actual_type = struct_info.GetChild(1).GetDuckType();
	// Override the duckdb type to the actual type
	type = actual_type;
	run_end_encoded = true;
}

bool ArrowType::RunEndEncoded() const {
	return run_end_encoded;
}

LogicalType ArrowType::GetDuckType(bool use_dictionary) const {
	if (use_dictionary && dictionary_type) {
		return dictionary_type->GetDuckType();
	}
	if (!use_dictionary) {
		return type;
	}
	// Dictionaries can exist in arbitrarily nested schemas
	// have to reconstruct the type
	auto id = type.id();
	switch (id) {
	case LogicalTypeId::STRUCT: {
		auto &struct_info = type_info->Cast<ArrowStructInfo>();
		child_list_t<LogicalType> new_children;
		for (idx_t i = 0; i < struct_info.ChildCount(); i++) {
			auto &child = struct_info.GetChild(i);
			auto &child_name = StructType::GetChildName(type, i);
			new_children.emplace_back(std::make_pair(child_name, child.GetDuckType(true)));
		}
		return LogicalType::STRUCT(std::move(new_children));
	}
	case LogicalTypeId::LIST: {
		auto &list_info = type_info->Cast<ArrowListInfo>();
		auto &child = list_info.GetChild();
		return LogicalType::LIST(child.GetDuckType(true));
	}
	case LogicalTypeId::MAP: {
		auto &list_info = type_info->Cast<ArrowListInfo>();
		auto &struct_child = list_info.GetChild();
		auto struct_type = struct_child.GetDuckType(true);
		return LogicalType::MAP(StructType::GetChildType(struct_type, 0), StructType::GetChildType(struct_type, 1));
	}
	case LogicalTypeId::UNION: {
		auto &union_info = type_info->Cast<ArrowStructInfo>();
		child_list_t<LogicalType> new_children;
		for (idx_t i = 0; i < union_info.ChildCount(); i++) {
			auto &child = union_info.GetChild(i);
			auto &child_name = UnionType::GetMemberName(type, i);
			new_children.emplace_back(std::make_pair(child_name, child.GetDuckType(true)));
		}
		return LogicalType::UNION(std::move(new_children));
	}
	default: {
		return type;
	}
	}
}

} // namespace duckdb
