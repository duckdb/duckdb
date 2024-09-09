#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/arrow/appender/fixed_size_list_data.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Arrays
//===--------------------------------------------------------------------===//
void ArrowFixedSizeListData::Initialize(ArrowAppendData &result, const LogicalType &type, idx_t capacity) {
	auto &child_type = ArrayType::GetChildType(type);
	auto array_size = ArrayType::GetSize(type);
	auto child_buffer = ArrowAppender::InitializeChild(child_type, capacity * array_size, result.options);
	result.child_data.push_back(std::move(child_buffer));
}

void ArrowFixedSizeListData::Append(ArrowAppendData &append_data, Vector &input, idx_t from, idx_t to,
                                    idx_t input_size) {
	UnifiedVectorFormat format;
	input.ToUnifiedFormat(input_size, format);
	idx_t size = to - from;
	AppendValidity(append_data, format, from, to);
	input.Flatten(input_size);
	auto array_size = ArrayType::GetSize(input.GetType());
	auto &child_vector = ArrayVector::GetEntry(input);
	auto &child_data = *append_data.child_data[0];
	child_data.append_vector(child_data, child_vector, from * array_size, to * array_size, size * array_size);
	append_data.row_count += size;
}

void ArrowFixedSizeListData::Finalize(ArrowAppendData &append_data, const LogicalType &type, ArrowArray *result) {
	result->n_buffers = 1;
	auto &child_type = ArrayType::GetChildType(type);
	ArrowAppender::AddChildren(append_data, 1);
	result->children = append_data.child_pointers.data();
	result->n_children = 1;
	append_data.child_arrays[0] = *ArrowAppender::FinalizeChild(child_type, std::move(append_data.child_data[0]));
}

} // namespace duckdb
