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

const ArrowType &ArrowType::GetDictionary() const {
	D_ASSERT(dictionary_type);
	return *dictionary_type;
}

const LogicalType &ArrowType::GetDuckType() const {
	return type;
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
