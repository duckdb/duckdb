#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/arrow/appender/struct_data.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Structs
//===--------------------------------------------------------------------===//
void ArrowStructData::Initialize(ArrowAppendData &result, const LogicalType &type, idx_t capacity) {
	auto &children = StructType::GetChildTypes(type);
	for (auto &child : children) {
		auto child_buffer = ArrowAppender::InitializeChild(child.second, capacity, result.options);
		result.child_data.push_back(std::move(child_buffer));
	}
}

void ArrowStructData::Append(ArrowAppendData &append_data, Vector &input, idx_t from, idx_t to, idx_t input_size) {
	UnifiedVectorFormat format;
	input.ToUnifiedFormat(input_size, format);
	idx_t size = to - from;
	AppendValidity(append_data, format, from, to);
	// append the children of the struct
	auto &children = StructVector::GetEntries(input);
	for (idx_t child_idx = 0; child_idx < children.size(); child_idx++) {
		auto &child = children[child_idx];
		auto &child_data = *append_data.child_data[child_idx];
		child_data.append_vector(child_data, *child, from, to, size);
	}
	append_data.row_count += size;
}

void ArrowStructData::Finalize(ArrowAppendData &append_data, const LogicalType &type, ArrowArray *result) {
	result->n_buffers = 1;

	auto &child_types = StructType::GetChildTypes(type);
	append_data.child_pointers.resize(child_types.size());
	result->children = append_data.child_pointers.data();
	result->n_children = child_types.size();
	for (idx_t i = 0; i < child_types.size(); i++) {
		auto &child_type = child_types[i].second;
		append_data.child_pointers[i] = ArrowAppender::FinalizeChild(child_type, *append_data.child_data[i]);
	}
}

} // namespace duckdb
