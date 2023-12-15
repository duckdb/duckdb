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

void ArrowType::AddChild(unique_ptr<ArrowType> child) {
	children.emplace_back(std::move(child));
}

void ArrowType::AssignChildren(vector<unique_ptr<ArrowType>> children) {
	D_ASSERT(this->children.empty());
	this->children = std::move(children);
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
		child_list_t<LogicalType> new_children;
		for (idx_t i = 0; i < children.size(); i++) {
			auto &child = children[i];
			auto &child_name = StructType::GetChildName(type, i);
			new_children.emplace_back(std::make_pair(child_name, child->GetDuckType(true)));
		}
		return LogicalType::STRUCT(std::move(new_children));
	}
	case LogicalTypeId::LIST: {
		auto &child = children[0];
		return LogicalType::LIST(child->GetDuckType(true));
	}
	case LogicalTypeId::MAP: {
		auto &struct_child = children[0];
		auto struct_type = struct_child->GetDuckType(true);
		return LogicalType::MAP(StructType::GetChildType(struct_type, 0), StructType::GetChildType(struct_type, 1));
	}
	case LogicalTypeId::UNION: {
		child_list_t<LogicalType> new_children;
		for (idx_t i = 0; i < children.size(); i++) {
			auto &child = children[i];
			auto &child_name = UnionType::GetMemberName(type, i);
			new_children.emplace_back(std::make_pair(child_name, child->GetDuckType(true)));
		}
		return LogicalType::UNION(std::move(new_children));
	}
	default: {
		return type;
	}
	}
}

ArrowVariableSizeType ArrowType::GetSizeType() const {
	return size_type;
}

ArrowDateTimeType ArrowType::GetDateTimeType() const {
	return date_time_precision;
}

const ArrowType &ArrowType::operator[](idx_t index) const {
	D_ASSERT(index < children.size());
	return *children[index];
}

idx_t ArrowType::FixedSize() const {
	D_ASSERT(size_type == ArrowVariableSizeType::FIXED_SIZE);
	return fixed_size;
}

} // namespace duckdb
