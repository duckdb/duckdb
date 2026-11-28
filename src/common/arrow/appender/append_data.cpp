#include "duckdb/common/arrow/appender/append_data.hpp"

#include "duckdb/common/type_visitor.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

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

//! Whether appending a value of this type can throw, which must not happen for a value nobody can see. This is the case
//! for a UHUGEINT exported as the decimal128 of format version 1.0, which cannot hold values of 2^127 and above
static bool AppendRejectsValues(const LogicalType &type, const ClientProperties &options) {
	if (options.arrow_lossless_conversion || options.arrow_output_version >= ArrowFormatVersion::V1_1) {
		return false;
	}
	return TypeVisitor::Contains(type, LogicalTypeId::UHUGEINT);
}

void ArrowAppendData::AppendChildOfParent(const Vector &child, const UnifiedVectorFormat &parent_format, idx_t from,
                                          idx_t to, idx_t input_size, idx_t child_count) {
	bool has_null_parent = false;
	if (!parent_format.validity.CannotHaveNull() && AppendRejectsValues(child.GetType(), options)) {
		for (idx_t i = from; i < to && !has_null_parent; i++) {
			has_null_parent = !parent_format.validity.RowIsValid(parent_format.sel->get_index(i));
		}
	}
	if (!has_null_parent) {
		AppendChild(child, from * child_count, to * child_count, input_size * child_count);
		return;
	}
	// copy the child values of these rows, so the input stays as it is, and hide the ones under a NULL parent
	const auto size = (to - from) * child_count;
	Vector hidden(child.GetType(), MaxValue<idx_t>(size, STANDARD_VECTOR_SIZE));
	VectorOperations::Copy(child, hidden, to * child_count, from * child_count, 0);
	for (idx_t i = from; i < to; i++) {
		if (parent_format.validity.RowIsValid(parent_format.sel->get_index(i))) {
			continue;
		}
		for (idx_t k = 0; k < child_count; k++) {
			FlatVector::SetNull(hidden, (i - from) * child_count + k, true);
		}
	}
	AppendChild(hidden, 0, size, size);
}

} // namespace duckdb
