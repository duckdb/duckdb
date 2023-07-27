#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/arrow/arrow.hpp"
#include "duckdb/common/arrow/arrow_buffer.hpp"
#include "duckdb/common/arrow/arrow_options.hpp"
#include "duckdb/common/array.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Arrow append data
//===--------------------------------------------------------------------===//
typedef void (*initialize_t)(ArrowAppendData &result, const LogicalType &type, idx_t capacity);
// append_data: The arrow array we're appending into
// input: The data we're appending
// from: The offset into the input we're scanning
// to: The last index of the input we're scanning
// input_size: The total size of the 'input' Vector.
typedef void (*append_vector_t)(ArrowAppendData &append_data, Vector &input, idx_t from, idx_t to, idx_t input_size);
typedef void (*finalize_t)(ArrowAppendData &append_data, const LogicalType &type, ArrowArray *result);

// This struct is used to save state for appending a column
// afterwards the ownership is passed to the arrow array, as 'private_data'
// FIXME: we should separate the append state variables from the variables required by the ArrowArray into
// ArrowAppendState
struct ArrowAppendData {
	explicit ArrowAppendData(ArrowOptions &options_p) : options(options_p) {
	}
	// the buffers of the arrow vector
	ArrowBuffer validity;
	ArrowBuffer main_buffer;
	ArrowBuffer aux_buffer;

	idx_t row_count = 0;
	idx_t null_count = 0;

	// function pointers for construction
	initialize_t initialize = nullptr;
	append_vector_t append_vector = nullptr;
	finalize_t finalize = nullptr;

	// child data (if any)
	vector<unique_ptr<ArrowAppendData>> child_data;

	// the arrow array C API data, only set after Finalize
	unique_ptr<ArrowArray> array;
	duckdb::array<const void *, 3> buffers = {{nullptr, nullptr, nullptr}};
	vector<ArrowArray *> child_pointers;

	ArrowOptions options;
};

//===--------------------------------------------------------------------===//
// Append Helper Functions
//===--------------------------------------------------------------------===//
static void GetBitPosition(idx_t row_idx, idx_t &current_byte, uint8_t &current_bit) {
	current_byte = row_idx / 8;
	current_bit = row_idx % 8;
}

static void UnsetBit(uint8_t *data, idx_t current_byte, uint8_t current_bit) {
	data[current_byte] &= ~((uint64_t)1 << current_bit);
}

static void NextBit(idx_t &current_byte, uint8_t &current_bit) {
	current_bit++;
	if (current_bit == 8) {
		current_byte++;
		current_bit = 0;
	}
}

static void ResizeValidity(ArrowBuffer &buffer, idx_t row_count) {
	auto byte_count = (row_count + 7) / 8;
	buffer.resize(byte_count, 0xFF);
}

static void SetNull(ArrowAppendData &append_data, uint8_t *validity_data, idx_t current_byte, uint8_t current_bit) {
	UnsetBit(validity_data, current_byte, current_bit);
	append_data.null_count++;
}

static void AppendValidity(ArrowAppendData &append_data, UnifiedVectorFormat &format, idx_t from, idx_t to) {
	// resize the buffer, filling the validity buffer with all valid values
	idx_t size = to - from;
	ResizeValidity(append_data.validity, append_data.row_count + size);
	if (format.validity.AllValid()) {
		// if all values are valid we don't need to do anything else
		return;
	}

	// otherwise we iterate through the validity mask
	auto validity_data = (uint8_t *)append_data.validity.data();
	uint8_t current_bit;
	idx_t current_byte;
	GetBitPosition(append_data.row_count, current_byte, current_bit);
	for (idx_t i = from; i < to; i++) {
		auto source_idx = format.sel->get_index(i);
		// append the validity mask
		if (!format.validity.RowIsValid(source_idx)) {
			SetNull(append_data, validity_data, current_byte, current_bit);
		}
		NextBit(current_byte, current_bit);
	}
}

} // namespace duckdb
