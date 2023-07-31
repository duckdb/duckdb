#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "duckdb/common/arrow/arrow.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

vector<LogicalType> ArrowTableType::GetDuckDBTypes() {
	vector<LogicalType> duckdb_types;
	for (auto &column : columns) {
		duckdb_types.emplace_back(column.GetDuckType());
	}
	return duckdb_types;
}

void ArrowType::AddChild(ArrowType &&child) {
	children.emplace_back(std::move(child));
}

void ArrowType::AssignChildren(vector<ArrowType> children) {
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
	return children[index];
}

idx_t ArrowType::FixedSize() const {
	D_ASSERT(size_type == ArrowVariableSizeType::FIXED_SIZE);
	return fixed_size;
}

void ArrowTableType::AddColumn(ArrowType &&column) {
	columns.emplace_back(std::move(column));
}

} // namespace duckdb
