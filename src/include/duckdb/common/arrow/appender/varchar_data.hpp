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

template <class SRC = string_t, class OP = ArrowVarcharConverter, class BUFTYPE = uint64_t>
struct ArrowVarcharData {
	static void Initialize(ArrowAppendData &result, const LogicalType &type, idx_t capacity) {
		result.main_buffer.reserve((capacity + 1) * sizeof(BUFTYPE));

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
		idx_t max_offset = append_data.row_count + to - from;
		if (max_offset > NumericLimits<uint32_t>::Maximum() &&
		    append_data.options.arrow_offset_size == ArrowOffsetSize::REGULAR) {
			throw InvalidInputException("Arrow Appender: The maximum total string size for regular string buffers is "
			                            "%u but the offset of %lu exceeds this.",
			                            NumericLimits<uint32_t>::Maximum(), max_offset);
		}
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
			offset_data[offset_idx] = current_offset;

			// resize the string buffer if required, and write the string data
			append_data.aux_buffer.resize(current_offset);
			OP::WriteData(append_data.aux_buffer.data() + last_offset, data[source_idx]);

			last_offset = current_offset;
		}
		append_data.row_count += size;
	}

	static void Finalize(ArrowAppendData &append_data, const LogicalType &type, ArrowArray *result) {
		result->n_buffers = 3;
		result->buffers[1] = append_data.main_buffer.data();
		result->buffers[2] = append_data.aux_buffer.data();
	}
};

} // namespace duckdb
