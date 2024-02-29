#include "duckdb/function/table/arrow/arrow_type_info.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// ArrowTypeInfo
//===--------------------------------------------------------------------===//

ArrowTypeInfo::ArrowTypeInfo(ArrowTypeInfoType type) : type(type) {
}

ArrowTypeInfo::~ArrowTypeInfo() {
}

//===--------------------------------------------------------------------===//
// ArrowStructInfo
//===--------------------------------------------------------------------===//

ArrowStructInfo::ArrowStructInfo(vector<unique_ptr<ArrowType>> children)
    : ArrowTypeInfo(ArrowTypeInfoType::STRUCT), children(std::move(children)) {
}

idx_t ArrowStructInfo::ChildCount() const {
	return children.size();
}

ArrowStructInfo::~ArrowStructInfo() {
}

const ArrowType &ArrowStructInfo::GetChild(idx_t index) const {
	D_ASSERT(index < children.size());
	return *children[index];
}

const vector<unique_ptr<ArrowType>> &ArrowStructInfo::GetChildren() const {
	return children;
}

//===--------------------------------------------------------------------===//
// ArrowDateTimeInfo
//===--------------------------------------------------------------------===//

ArrowDateTimeInfo::ArrowDateTimeInfo(ArrowDateTimeType size)
    : ArrowTypeInfo(ArrowTypeInfoType::DATE_TIME), size_type(size) {
}

ArrowDateTimeInfo::~ArrowDateTimeInfo() {
}

ArrowDateTimeType ArrowDateTimeInfo::GetDateTimeType() const {
	return size_type;
}

//===--------------------------------------------------------------------===//
// ArrowStringInfo
//===--------------------------------------------------------------------===//

ArrowStringInfo::ArrowStringInfo(ArrowVariableSizeType size)
    : ArrowTypeInfo(ArrowTypeInfoType::STRING), size_type(size), fixed_size(0) {
	D_ASSERT(size != ArrowVariableSizeType::FIXED_SIZE);
}

ArrowStringInfo::~ArrowStringInfo() {
}

ArrowStringInfo::ArrowStringInfo(idx_t fixed_size)
    : ArrowTypeInfo(ArrowTypeInfoType::STRING), size_type(ArrowVariableSizeType::FIXED_SIZE), fixed_size(fixed_size) {
}

ArrowVariableSizeType ArrowStringInfo::GetSizeType() const {
	return size_type;
}

idx_t ArrowStringInfo::FixedSize() const {
	D_ASSERT(size_type == ArrowVariableSizeType::FIXED_SIZE);
	return fixed_size;
}

//===--------------------------------------------------------------------===//
// ArrowListInfo
//===--------------------------------------------------------------------===//

ArrowListInfo::ArrowListInfo(unique_ptr<ArrowType> child, ArrowVariableSizeType size)
    : ArrowTypeInfo(ArrowTypeInfoType::LIST), size_type(size), child(std::move(child)) {
}

ArrowListInfo::~ArrowListInfo() {
}

unique_ptr<ArrowListInfo> ArrowListInfo::ListView(unique_ptr<ArrowType> child, ArrowVariableSizeType size) {
	D_ASSERT(size == ArrowVariableSizeType::SUPER_SIZE || size == ArrowVariableSizeType::NORMAL);
	auto list_info = unique_ptr<ArrowListInfo>(new ArrowListInfo(std::move(child), size));
	list_info->is_view = true;
	return list_info;
}

unique_ptr<ArrowListInfo> ArrowListInfo::List(unique_ptr<ArrowType> child, ArrowVariableSizeType size) {
	D_ASSERT(size == ArrowVariableSizeType::SUPER_SIZE || size == ArrowVariableSizeType::NORMAL);
	return unique_ptr<ArrowListInfo>(new ArrowListInfo(std::move(child), size));
}

ArrowVariableSizeType ArrowListInfo::GetSizeType() const {
	return size_type;
}

bool ArrowListInfo::IsView() const {
	return is_view;
}

ArrowType &ArrowListInfo::GetChild() const {
	return *child;
}

//===--------------------------------------------------------------------===//
// ArrowArrayInfo
//===--------------------------------------------------------------------===//

ArrowArrayInfo::ArrowArrayInfo(unique_ptr<ArrowType> child, idx_t fixed_size)
    : ArrowTypeInfo(ArrowTypeInfoType::ARRAY), child(std::move(child)), fixed_size(fixed_size) {
	D_ASSERT(fixed_size > 0);
}

ArrowArrayInfo::~ArrowArrayInfo() {
}

idx_t ArrowArrayInfo::FixedSize() const {
	return fixed_size;
}

ArrowType &ArrowArrayInfo::GetChild() const {
	return *child;
}

} // namespace duckdb
