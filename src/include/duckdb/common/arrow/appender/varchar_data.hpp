//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/arrow/appender/varchar_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/arrow/appender/append_data.hpp"
#include "duckdb/common/arrow/appender/scalar_data.hpp"
#include "duckdb/common/types/arrow_string_view_type.hpp"
#include "duckdb/common/types/uuid.hpp"

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
		result.GetMainBuffer().reserve((capacity + 1) * sizeof(BUFTYPE));
		result.GetAuxBuffer().reserve(capacity);
	}

	template <bool LARGE_STRING>
	static void AppendTemplated(ArrowAppendData &append_data, Vector &input, idx_t from, idx_t to, idx_t input_size) {
		idx_t size = to - from;
		UnifiedVectorFormat format;
		input.ToUnifiedFormat(input_size, format);
		auto &main_buffer = append_data.GetMainBuffer();
		auto &validity_buffer = append_data.GetValidityBuffer();
		auto &aux_buffer = append_data.GetAuxBuffer();

		// resize the validity mask and set up the validity buffer for iteration
		ResizeValidity(validity_buffer, append_data.row_count + size);
		auto validity_data = (uint8_t *)validity_buffer.data();

		// resize the offset buffer - the offset buffer holds the offsets into the child array
		main_buffer.resize(main_buffer.size() + sizeof(BUFTYPE) * (size + 1));
		auto data = UnifiedVectorFormat::GetData<SRC>(format);
		auto offset_data = main_buffer.GetData<BUFTYPE>();
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
			auto current_offset = UnsafeNumericCast<idx_t>(last_offset) + string_length;
			if (!LARGE_STRING &&
			    UnsafeNumericCast<idx_t>(last_offset) + string_length > NumericLimits<int32_t>::Maximum()) {
				D_ASSERT(append_data.options.arrow_offset_size == ArrowOffsetSize::REGULAR);
				throw InvalidInputException(
				    "Arrow Appender: The maximum total string size for regular string buffers is "
				    "%u but the offset of %lu exceeds this.\n* SET arrow_large_buffer_size=true to use large string "
				    "buffers",
				    NumericLimits<int32_t>::Maximum(), current_offset);
			}
			offset_data[offset_idx] = UnsafeNumericCast<BUFTYPE>(current_offset);

			// resize the string buffer if required, and write the string data
			aux_buffer.resize(current_offset);
			OP::WriteData(aux_buffer.data() + last_offset, data[source_idx]);

			last_offset = UnsafeNumericCast<BUFTYPE>(current_offset);
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
		result->buffers[1] = append_data.GetMainBuffer().data();
		result->buffers[2] = append_data.GetAuxBuffer().data();
	}
};

struct ArrowVarcharToStringViewData {
	static void Initialize(ArrowAppendData &result, const LogicalType &type, idx_t capacity) {
		result.GetMainBuffer().reserve((capacity) * sizeof(arrow_string_view_t));
		result.GetAuxBuffer().reserve(capacity);
		result.GetBufferSizeBuffer().reserve(sizeof(int64_t));
	}

	static void Append(ArrowAppendData &append_data, Vector &input, idx_t from, idx_t to, idx_t input_size) {
		idx_t size = to - from;
		UnifiedVectorFormat format;
		input.ToUnifiedFormat(input_size, format);
		auto &main_buffer = append_data.GetMainBuffer();
		auto &validity_buffer = append_data.GetValidityBuffer();
		auto &aux_buffer = append_data.GetAuxBuffer();
		// resize the validity mask and set up the validity buffer for iteration
		ResizeValidity(validity_buffer, append_data.row_count + size);
		auto validity_data = (uint8_t *)validity_buffer.data();

		main_buffer.resize(main_buffer.size() + sizeof(arrow_string_view_t) * (size));
		// resize the offset buffer - the offset buffer holds the offsets into the child array
		auto data = UnifiedVectorFormat::GetData<string_t>(format);
		for (idx_t i = from; i < to; i++) {
			auto result_idx = append_data.row_count + i - from;
			auto arrow_data = main_buffer.GetData<arrow_string_view_t>();
			auto source_idx = format.sel->get_index(i);
			if (!format.validity.RowIsValid(source_idx)) {
				// Null value
				uint8_t current_bit;
				idx_t current_byte;
				GetBitPosition(result_idx, current_byte, current_bit);
				SetNull(append_data, validity_data, current_byte, current_bit);
				// We have to set these bytes to 0, for some reason
				arrow_data[result_idx] = arrow_string_view_t(0, "");
				continue;
			}
			// These two are now the same buffer
			idx_t string_length = ArrowVarcharConverter::GetLength(data[source_idx]);
			auto string_data = data[source_idx].GetData();
			if (string_length <= ArrowStringViewConstants::MAX_INLINED_BYTES) {
				//	This string is inlined
				//  | Bytes 0-3  | Bytes 4-15                            |
				//  |------------|---------------------------------------|
				//  | length     | data (padded with 0)                  |
				arrow_data[result_idx] = arrow_string_view_t(UnsafeNumericCast<int32_t>(string_length), string_data);
			} else {
				// This string is not inlined, we have to check a different buffer and offsets
				//  | Bytes 0-3  | Bytes 4-7  | Bytes 8-11 | Bytes 12-15 |
				//  |------------|------------|------------|-------------|
				//  | length     | prefix     | buf. index | offset      |
				arrow_data[result_idx] = arrow_string_view_t(UnsafeNumericCast<int32_t>(string_length), string_data, 0,
				                                             UnsafeNumericCast<int32_t>(append_data.offset));
				auto current_offset = append_data.offset + string_length;
				aux_buffer.resize(current_offset);
				ArrowVarcharConverter::WriteData(aux_buffer.data() + append_data.offset, data[source_idx]);
				append_data.offset = current_offset;
			}
		}
		append_data.row_count += size;
	}

	static void Finalize(ArrowAppendData &append_data, const LogicalType &type, ArrowArray *result) {
		// We output four buffers
		result->n_buffers = 4;
		// Buffer 0 is the validity mask
		// Buffer 1 is our string views (short/long strings)
		result->buffers[1] = append_data.GetMainBuffer().data();
		// Buffer 2 is our only data buffer, could theoretically be more [ buffers ]
		result->buffers[2] = append_data.GetAuxBuffer().data();
		// Buffer 3 is the data-buffer lengths buffer, and we also populate it in to finalize
		reinterpret_cast<int64_t *>(append_data.GetBufferSizeBuffer().data())[0] =
		    UnsafeNumericCast<int64_t>(append_data.offset);
		result->buffers[3] = append_data.GetBufferSizeBuffer().data();
	}
};

} // namespace duckdb
