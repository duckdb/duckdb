#include "duckdb/common/arrow/appender/append_data.hpp"

namespace duckdb {

void ArrowAppendData::AppendValidity(UnifiedVectorFormat &format, idx_t from, idx_t to) {
	// resize the buffer, filling the validity buffer with all valid values
	idx_t size = to - from;
	ResizeValidity(GetValidityBuffer(), row_count + size);
	if (format.validity.AllValid()) {
		// if all values are valid we don't need to do anything else
		return;
	}

	// otherwise we iterate through the validity mask
	auto validity_data = (uint8_t *)GetValidityBuffer().data();
	uint8_t current_bit;
	idx_t current_byte;
	GetBitPosition(row_count, current_byte, current_bit);
	for (idx_t i = from; i < to; i++) {
		auto source_idx = format.sel->get_index(i);
		// append the validity mask
		if (!format.validity.RowIsValid(source_idx)) {
			SetNull(validity_data, current_byte, current_bit);
		}
		NextBit(current_byte, current_bit);
	}
}

} // namespace duckdb
