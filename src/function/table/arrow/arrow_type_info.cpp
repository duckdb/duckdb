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

ArrowStructInfo::ArrowStructInfo(vector<shared_ptr<ArrowType>> children)
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

const vector<shared_ptr<ArrowType>> &ArrowStructInfo::GetChildren() const {
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
// ArrowDecimalInfo
//===--------------------------------------------------------------------===//

ArrowDecimalInfo::ArrowDecimalInfo(DecimalBitWidth bit_width)
    : ArrowTypeInfo(ArrowTypeInfoType::DECIMAL), bit_width(bit_width) {
}

ArrowDecimalInfo::~ArrowDecimalInfo() {
}

DecimalBitWidth ArrowDecimalInfo::GetBitWidth() const {
	return bit_width;
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

ArrowListInfo::ArrowListInfo(shared_ptr<ArrowType> child, ArrowVariableSizeType size)
    : ArrowTypeInfo(ArrowTypeInfoType::LIST), size_type(size), child(std::move(child)) {
}

ArrowListInfo::~ArrowListInfo() {
}

unique_ptr<ArrowListInfo> ArrowListInfo::ListView(shared_ptr<ArrowType> child, ArrowVariableSizeType size) {
	D_ASSERT(size == ArrowVariableSizeType::SUPER_SIZE || size == ArrowVariableSizeType::NORMAL);
	auto list_info = unique_ptr<ArrowListInfo>(new ArrowListInfo(std::move(child), size));
	list_info->is_view = true;
	return list_info;
}

unique_ptr<ArrowListInfo> ArrowListInfo::List(shared_ptr<ArrowType> child, ArrowVariableSizeType size) {
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
// ArrowUnionInfo
//===--------------------------------------------------------------------===//

ArrowUnionInfo::ArrowUnionInfo(vector<shared_ptr<ArrowType>> children, vector<int8_t> type_ids)
    : ArrowTypeInfo(ArrowTypeInfoType::UNION), children(std::move(children)) {
	// Build the reverse mapping: type_id -> child index
	// Arrow type IDs are non-negative int8_t values (0-127)
	type_id_to_child_idx.resize(128, DConstants::INVALID_INDEX);
	for (idx_t child_idx = 0; child_idx < type_ids.size(); child_idx++) {
		auto id = type_ids[child_idx];
		D_ASSERT(id >= 0);
		type_id_to_child_idx[static_cast<idx_t>(id)] = child_idx;
	}
}

ArrowUnionInfo::~ArrowUnionInfo() {
}

idx_t ArrowUnionInfo::ChildCount() const {
	return children.size();
}

const ArrowType &ArrowUnionInfo::GetChild(idx_t index) const {
	D_ASSERT(index < children.size());
	return *children[index];
}

const vector<shared_ptr<ArrowType>> &ArrowUnionInfo::GetChildren() const {
	return children;
}

idx_t ArrowUnionInfo::TypeIdToChildIndex(int8_t type_id) const {
	if (type_id < 0 || static_cast<idx_t>(type_id) >= type_id_to_child_idx.size()) {
		throw InvalidInputException("Arrow union type_id out of range: %d", static_cast<int>(type_id));
	}
	auto child_idx = type_id_to_child_idx[static_cast<idx_t>(type_id)];
	if (child_idx == DConstants::INVALID_INDEX) {
		throw InvalidInputException("Arrow union type_id %d does not map to any child", static_cast<int>(type_id));
	}
	return child_idx;
}

//===--------------------------------------------------------------------===//
// ArrowArrayInfo
//===--------------------------------------------------------------------===//

ArrowArrayInfo::ArrowArrayInfo(shared_ptr<ArrowType> child, idx_t fixed_size)
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
