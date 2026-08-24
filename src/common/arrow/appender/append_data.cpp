#include "duckdb/common/arrow/appender/append_data.hpp"

namespace duckdb {

void ArrowAppendData::AppendValidity(UnifiedVectorFormat &format, idx_t from, idx_t to) {
	// resize the buffer, filling the validity buffer with all valid values
	idx_t size = to - from;
	ResizeValidity(GetValidityBuffer(), row_count + size);
	if (format.validity.CannotHaveNull()) {
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

void ArrowAppendData::AppendChild(const Vector &input, idx_t from, idx_t to, idx_t input_size) {
	if (extension_data && extension_data->duckdb_to_arrow) {
		// Convert the DuckDB-typed input into the extension's internal Arrow type before
		// handing it to the (internal-typed) child appender. Size the internal vector to the
		// actual input_size: container children can exceed STANDARD_VECTOR_SIZE (e.g. a 2048-row
		// LIST whose elements average two entries), and duckdb_to_arrow writes input_size values.
		Vector internal(extension_data->GetInternalType(), MaxValue<idx_t>(input_size, STANDARD_VECTOR_SIZE));
		extension_data->duckdb_to_arrow(*options.client_context, input, internal, input_size);
		append_vector(*this, internal, from, to, input_size);
	} else {
		append_vector(*this, input, from, to, input_size);
	}
}

} // namespace duckdb
