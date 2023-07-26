#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/arrow/appender/map_data.hpp"
#include "duckdb/common/arrow/appender/list_data.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Maps
//===--------------------------------------------------------------------===//
void ArrowMapData::Initialize(ArrowAppendData &result, const LogicalType &type, idx_t capacity) {
	// map types are stored in a (too) clever way
	// the main buffer holds the null values and the offsets
	// then we have a single child, which is a struct of the map_type, and the key_type
	result.main_buffer.reserve((capacity + 1) * sizeof(uint32_t));

	auto &key_type = MapType::KeyType(type);
	auto &value_type = MapType::ValueType(type);
	auto internal_struct = make_uniq<ArrowAppendData>(result.options);
	internal_struct->child_data.push_back(ArrowAppender::InitializeChild(key_type, capacity, result.options));
	internal_struct->child_data.push_back(ArrowAppender::InitializeChild(value_type, capacity, result.options));

	result.child_data.push_back(std::move(internal_struct));
}

void ArrowMapData::Append(ArrowAppendData &append_data, Vector &input, idx_t from, idx_t to, idx_t input_size) {
	UnifiedVectorFormat format;
	input.ToUnifiedFormat(input_size, format);
	idx_t size = to - from;
	AppendValidity(append_data, format, from, to);
	vector<sel_t> child_indices;
	ArrowListData::AppendOffsets(append_data, format, from, to, child_indices);

	SelectionVector child_sel(child_indices.data());
	auto &key_vector = MapVector::GetKeys(input);
	auto &value_vector = MapVector::GetValues(input);
	auto list_size = child_indices.size();

	auto &struct_data = *append_data.child_data[0];
	auto &key_data = *struct_data.child_data[0];
	auto &value_data = *struct_data.child_data[1];

	Vector key_vector_copy(key_vector.GetType());
	key_vector_copy.Slice(key_vector, child_sel, list_size);
	Vector value_vector_copy(value_vector.GetType());
	value_vector_copy.Slice(value_vector, child_sel, list_size);
	key_data.append_vector(key_data, key_vector_copy, 0, list_size, list_size);
	value_data.append_vector(value_data, value_vector_copy, 0, list_size, list_size);

	append_data.row_count += size;
	struct_data.row_count += size;
}

void ArrowMapData::Finalize(ArrowAppendData &append_data, const LogicalType &type, ArrowArray *result) {
	// set up the main map buffer
	result->n_buffers = 2;
	result->buffers[1] = append_data.main_buffer.data();

	// the main map buffer has a single child: a struct
	append_data.child_pointers.resize(1);
	result->children = append_data.child_pointers.data();
	result->n_children = 1;
	append_data.child_pointers[0] = ArrowAppender::FinalizeChild(type, *append_data.child_data[0]);

	// now that struct has two children: the key and the value type
	auto &struct_data = *append_data.child_data[0];
	auto &struct_result = append_data.child_pointers[0];
	struct_data.child_pointers.resize(2);
	struct_result->n_buffers = 1;
	struct_result->n_children = 2;
	struct_result->length = struct_data.child_data[0]->row_count;
	struct_result->children = struct_data.child_pointers.data();

	D_ASSERT(struct_data.child_data[0]->row_count == struct_data.child_data[1]->row_count);

	auto &key_type = MapType::KeyType(type);
	auto &value_type = MapType::ValueType(type);
	struct_data.child_pointers[0] = ArrowAppender::FinalizeChild(key_type, *struct_data.child_data[0]);
	struct_data.child_pointers[1] = ArrowAppender::FinalizeChild(value_type, *struct_data.child_data[1]);

	// keys cannot have null values
	if (struct_data.child_pointers[0]->null_count > 0) {
		throw std::runtime_error("Arrow doesn't accept NULL keys on Maps");
	}
}

} // namespace duckdb
