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

void ArrowAppendData::AppendChild(Vector &input, idx_t from, idx_t to, idx_t input_size) {
	if (extension_data && extension_data->duckdb_to_arrow) {
		// Mirror the top-level ArrowAppender::Append: convert the DuckDB-typed
		// vector into the extension's internal Arrow-typed vector before
		// handing it to the (internal-typed) child appender. The conversion
		// runs over the full `input_size` to match `append_vector`'s
		// expectations of a fully-resolved input vector.
		//
		// Flatten first because some duckdb_to_arrow callbacks (e.g.
		// ArrowBool8::DuckToArrow) read `format.data[i]` without honoring
		// `format.sel`, so feeding them a sliced/dictionary vector reads out
		// of bounds. Container appenders routinely produce sliced child
		// vectors, so the conversion path can't rely on the source being
		// flat the way the top-level append path does.
		input.Flatten(input_size);
		// Allocate the internal vector with at least input_size capacity:
		// container appenders can produce child vectors larger than
		// STANDARD_VECTOR_SIZE (e.g. a 2048-row LIST whose elements average
		// 2 entries → 4096-element child), and several duckdb_to_arrow
		// callbacks write input_size values into the result without
		// bounds-checking.
		Vector internal(extension_data->GetInternalType(), MaxValue<idx_t>(input_size, STANDARD_VECTOR_SIZE));
		extension_data->duckdb_to_arrow(*options.client_context, input, internal, input_size);
		append_vector(*this, internal, from, to, input_size);
	} else {
		append_vector(*this, input, from, to, input_size);
	}
}

} // namespace duckdb
