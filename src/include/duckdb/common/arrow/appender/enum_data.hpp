#pragma once

#include "duckdb/common/arrow/appender/append_data.hpp"
#include "duckdb/common/arrow/appender/scalar_data.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Enums
//===--------------------------------------------------------------------===//
template <class TGT>
struct ArrowEnumData : public ArrowScalarBaseData<TGT> {
	static idx_t GetLength(string_t input) {
		return input.GetSize();
	}
	static void WriteData(data_ptr_t target, string_t input) {
		memcpy(target, input.GetData(), input.GetSize());
	}
	static void EnumAppendVector(ArrowAppendData &append_data, const Vector &input, idx_t size) {
		D_ASSERT(input.GetVectorType() == VectorType::FLAT_VECTOR);

		// resize the validity mask and set up the validity buffer for iteration
		ResizeValidity(append_data.validity, append_data.row_count + size);

		// resize the offset buffer - the offset buffer holds the offsets into the child array
		append_data.main_buffer.resize(append_data.main_buffer.size() + sizeof(uint32_t) * (size + 1));
		auto data = FlatVector::GetData<string_t>(input);
		auto offset_data = append_data.main_buffer.GetData<uint32_t>();
		if (append_data.row_count == 0) {
			// first entry
			offset_data[0] = 0;
		}
		// now append the string data to the auxiliary buffer
		// the auxiliary buffer's length depends on the string lengths, so we resize as required
		auto last_offset = offset_data[append_data.row_count];
		for (idx_t i = 0; i < size; i++) {
			auto offset_idx = append_data.row_count + i + 1;

			auto string_length = GetLength(data[i]);

			// append the offset data
			auto current_offset = last_offset + string_length;
			offset_data[offset_idx] = current_offset;

			// resize the string buffer if required, and write the string data
			append_data.aux_buffer.resize(current_offset);
			WriteData(append_data.aux_buffer.data() + last_offset, data[i]);

			last_offset = current_offset;
		}
		append_data.row_count += size;
	}
	static void Initialize(ArrowAppendData &result, const LogicalType &type, idx_t capacity) {
		result.main_buffer.reserve(capacity * sizeof(TGT));
		// construct the enum child data
		auto enum_data = ArrowAppender::InitializeChild(LogicalType::VARCHAR, EnumType::GetSize(type), result.options);
		EnumAppendVector(*enum_data, EnumType::GetValuesInsertOrder(type), EnumType::GetSize(type));
		result.child_data.push_back(std::move(enum_data));
	}

	static void Finalize(ArrowAppendData &append_data, const LogicalType &type, ArrowArray *result) {
		result->n_buffers = 2;
		result->buffers[1] = append_data.main_buffer.data();
		// finalize the enum child data, and assign it to the dictionary
		result->dictionary = ArrowAppender::FinalizeChild(LogicalType::VARCHAR, *append_data.child_data[0]);
	}
};

} // namespace duckdb
