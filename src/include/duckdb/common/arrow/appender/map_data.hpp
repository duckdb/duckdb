#pragma once

#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/arrow/appender/append_data.hpp"
#include "duckdb/common/arrow/appender/list_data.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Maps
//===--------------------------------------------------------------------===//
template <class BUFTYPE = int64_t>
struct ArrowMapData {
public:
	static void Initialize(ArrowAppendData &result, const LogicalType &type, idx_t capacity) {
		// map types are stored in a (too) clever way
		// the main buffer holds the null values and the offsets
		// then we have a single child, which is a struct of the map_type, and the key_type
		result.GetMainBuffer().reserve((capacity + 1) * sizeof(BUFTYPE));

		auto &key_type = MapType::KeyType(type);
		auto &value_type = MapType::ValueType(type);
		auto internal_struct = make_uniq<ArrowAppendData>(result.options);
		internal_struct->child_data.push_back(ArrowAppender::InitializeChild(key_type, capacity, result.options));
		internal_struct->child_data.push_back(ArrowAppender::InitializeChild(value_type, capacity, result.options));

		result.child_data.push_back(std::move(internal_struct));
	}

	static void Append(ArrowAppendData &append_data, Vector &input, idx_t from, idx_t to, idx_t input_size) {
		UnifiedVectorFormat format;
		input.ToUnifiedFormat(input_size, format);
		idx_t size = to - from;
		AppendValidity(append_data, format, from, to);
		vector<sel_t> child_indices;
		ArrowListData<BUFTYPE>::AppendOffsets(append_data, format, from, to, child_indices);

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

	static void Finalize(ArrowAppendData &append_data, const LogicalType &type, ArrowArray *result) {
		// set up the main map buffer
		D_ASSERT(result);
		result->n_buffers = 2;
		result->buffers[1] = append_data.GetMainBuffer().data();

		// the main map buffer has a single child: a struct
		ArrowAppender::AddChildren(append_data, 1);
		result->children = append_data.child_pointers.data();
		result->n_children = 1;

		auto &struct_data = *append_data.child_data[0];
		auto struct_result = ArrowAppender::FinalizeChild(type, std::move(append_data.child_data[0]));

		// Initialize the struct array data
		const auto struct_child_count = 2;
		ArrowAppender::AddChildren(struct_data, struct_child_count);
		struct_result->children = struct_data.child_pointers.data();
		struct_result->n_buffers = 1;
		struct_result->n_children = struct_child_count;
		struct_result->length = NumericCast<int64_t>(struct_data.child_data[0]->row_count);

		append_data.child_arrays[0] = *struct_result;

		D_ASSERT(struct_data.child_data[0]->row_count == struct_data.child_data[1]->row_count);

		auto &key_type = MapType::KeyType(type);
		auto &value_type = MapType::ValueType(type);
		auto key_data = ArrowAppender::FinalizeChild(key_type, std::move(struct_data.child_data[0]));
		struct_data.child_arrays[0] = *key_data;
		struct_data.child_arrays[1] = *ArrowAppender::FinalizeChild(value_type, std::move(struct_data.child_data[1]));

		// keys cannot have null values
		if (key_data->null_count > 0) {
			throw std::runtime_error("Arrow doesn't accept NULL keys on Maps");
		}
	}
};

} // namespace duckdb
