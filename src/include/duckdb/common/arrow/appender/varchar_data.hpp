#pragma once

#include "duckdb/common/arrow/appender/append_data.hpp"
#include "duckdb/common/arrow/appender/scalar_data.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Varchar
//===--------------------------------------------------------------------===//
struct ArrowVarcharConverter {
	template <class SRC>
	static idx_t GetLength(SRC input) {
		return input.GetSize();
	}

	template <class SRC>
	static void WriteData(data_ptr_t target, SRC input) {
		memcpy(target, input.GetData(), input.GetSize());
	}
};

struct ArrowUUIDConverter {
	template <class SRC>
	static idx_t GetLength(SRC input) {
		return UUID::STRING_SIZE;
	}

	template <class SRC>
	static void WriteData(data_ptr_t target, SRC input) {
		UUID::ToString(input, char_ptr_cast(target));
	}
};

template <class SRC = string_t, class OP = ArrowVarcharConverter, class BUFTYPE = int64_t>
struct ArrowVarcharData {
	static void Initialize(ArrowAppendData &result, const LogicalType &type, idx_t capacity) {
		result.main_buffer.reserve((capacity + 1) * sizeof(BUFTYPE));

		result.aux_buffer.reserve(capacity);
	}

	template <bool LARGE_STRING>
	static void AppendTemplated(ArrowAppendData &append_data, Vector &input, idx_t from, idx_t to, idx_t input_size) {
		idx_t size = to - from;
		UnifiedVectorFormat format;
		input.ToUnifiedFormat(input_size, format);

		// resize the validity mask and set up the validity buffer for iteration
		ResizeValidity(append_data.validity, append_data.row_count + size);
		auto validity_data = (uint8_t *)append_data.validity.data();

		// resize the offset buffer - the offset buffer holds the offsets into the child array
		append_data.main_buffer.resize(append_data.main_buffer.size() + sizeof(BUFTYPE) * (size + 1));
		auto data = UnifiedVectorFormat::GetData<SRC>(format);
		auto offset_data = append_data.main_buffer.GetData<BUFTYPE>();
		if (append_data.row_count == 0) {
			// first entry
			offset_data[0] = 0;
		}
		// now append the string data to the auxiliary buffer
		// the auxiliary buffer's length depends on the string lengths, so we resize as required
		auto last_offset = offset_data[append_data.row_count];
		for (idx_t i = from; i < to; i++) {
			auto source_idx = format.sel->get_index(i);
			auto offset_idx = append_data.row_count + i + 1 - from;

			if (!format.validity.RowIsValid(source_idx)) {
				uint8_t current_bit;
				idx_t current_byte;
				GetBitPosition(append_data.row_count + i - from, current_byte, current_bit);
				SetNull(append_data, validity_data, current_byte, current_bit);
				offset_data[offset_idx] = last_offset;
				continue;
			}

			auto string_length = OP::GetLength(data[source_idx]);

			// append the offset data
			auto current_offset = last_offset + string_length;
			if (!LARGE_STRING && (int64_t)last_offset + string_length > NumericLimits<int32_t>::Maximum()) {
				D_ASSERT(append_data.options.arrow_offset_size == ArrowOffsetSize::REGULAR);
				throw InvalidInputException(
				    "Arrow Appender: The maximum total string size for regular string buffers is "
				    "%u but the offset of %lu exceeds this.",
				    NumericLimits<int32_t>::Maximum(), current_offset);
			}
			offset_data[offset_idx] = current_offset;

			// resize the string buffer if required, and write the string data
			append_data.aux_buffer.resize(current_offset);
			OP::WriteData(append_data.aux_buffer.data() + last_offset, data[source_idx]);

			last_offset = current_offset;
		}
		append_data.row_count += size;
	}

	static void Append(ArrowAppendData &append_data, Vector &input, idx_t from, idx_t to, idx_t input_size) {
		if (append_data.options.arrow_offset_size == ArrowOffsetSize::REGULAR) {
			// Check if the offset exceeds the max supported value
			AppendTemplated<false>(append_data, input, from, to, input_size);
		} else {
			AppendTemplated<true>(append_data, input, from, to, input_size);
		}
	}

	static void Finalize(ArrowAppendData &append_data, const LogicalType &type, ArrowArray *result) {
		result->n_buffers = 3;
		result->buffers[1] = append_data.main_buffer.data();
		result->buffers[2] = append_data.aux_buffer.data();
	}
};

struct ArrowVarcharToStringViewData {
	static void Initialize(ArrowAppendData &result, const LogicalType &type, idx_t capacity) {
		result.main_buffer.reserve((capacity + 1) * 16);
		result.aux_buffer.reserve(capacity);
	}

	static void Append(ArrowAppendData &append_data, Vector &input, idx_t from, idx_t to, idx_t input_size) {
		idx_t size = to - from;
		UnifiedVectorFormat format;
		input.ToUnifiedFormat(input_size, format);

		// resize the validity mask and set up the validity buffer for iteration
		ResizeValidity(append_data.validity, append_data.row_count + size);
		auto validity_data = (uint8_t *)append_data.validity.data();

		// resize the offset buffer - the offset buffer holds the offsets into the child array
		auto data = UnifiedVectorFormat::GetData<string_t>(format);
		idx_t last_offset = 0;
		for (idx_t i = from; i < to; i++) {
			auto source_idx = format.sel->get_index(i);

			if (!format.validity.RowIsValid(source_idx)) {
				// Null value
				uint8_t current_bit;
				idx_t current_byte;
				GetBitPosition(append_data.row_count + i - from, current_byte, current_bit);
				SetNull(append_data, validity_data, current_byte, current_bit);
				continue;
			}

			// These two are now the same buffer
			auto arrow_4byte = append_data.main_buffer.GetData<int32_t>();
			auto string_data = append_data.main_buffer.GetData<char>();
			// This is the 16 byte index
			auto result_idx = append_data.row_count + i - from;
			auto &string_length = arrow_4byte[result_idx * 4];
			string_length = ArrowVarcharConverter::GetLength(data[source_idx]);

			if (string_length <= 12) {
				//	This string is inlined
				//  | Bytes 0-3  | Bytes 4-15                            |
				//  |------------|---------------------------------------|
				//  | length     | data (padded with 0)                  |
				memcpy(&string_data[i * 16 + 4], data[source_idx].GetData(), string_length);
			} else {
				//* This string is not inlined, we have to check a different buffer and offsets
				//  | Bytes 0-3  | Bytes 4-7  | Bytes 8-11 | Bytes 12-15 |
				//  |------------|------------|------------|-------------|
				//  | length     | prefix     | buf. index | offset      |
				// Copy Prefix
				memcpy(&string_data[i * 16 + 4], data[source_idx].GetPrefix(), 4);
				// Produced Data Buffer is always 0
				arrow_4byte[result_idx * 6] = 0;
				// Give Offset
				arrow_4byte[result_idx * 7] = last_offset;
				// Copy data to data buffer
				auto current_offset = last_offset + string_length;
				append_data.aux_buffer.resize(current_offset);
				memcpy(append_data.aux_buffer.data() + last_offset, data[source_idx].GetData(), string_length);
				last_offset = current_offset;
			}
		}
		append_data.row_count += size;
	}

	static void Finalize(ArrowAppendData &append_data, const LogicalType &type, ArrowArray *result) {
		result->n_buffers = 2;
		result->buffers[1] = append_data.main_buffer.data();
		//		result->buffers[2] = append_data.aux_buffer.data();
	}
};

} // namespace duckdb
