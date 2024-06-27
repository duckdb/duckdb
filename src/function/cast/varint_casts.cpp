#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/function/cast/vector_cast_helpers.hpp"

namespace duckdb {
constexpr uint8_t VARINT_HEADER_SIZE = 3;

string_t IntToVarInt(int32_t int_value) {
	// Determine if the number is negative
	bool is_negative = int_value < 0;

	// If negative, convert to two's complement
	if (is_negative) {
		int_value = ~(-int_value) + 1;
	}

	// Determine the number of data bytes
	uint64_t abs_value = std::abs(int_value);
	uint32_t data_byte_size = (abs_value == 0) ? 1 : static_cast<int>(std::ceil(std::log2(abs_value + 1) / 8.0));

	// Create the header
	uint32_t header = data_byte_size;
	if (!is_negative) {
		header |= (1 << 23); // Set the sign bit for positive numbers
	}

	string_t blob {data_byte_size + VARINT_HEADER_SIZE};

	auto writable_blob = blob.GetDataWriteable();
	// Add header bytes to the blob
	idx_t wb_idx = 0;
	writable_blob[wb_idx++] = static_cast<uint8_t>(header >> 16);
	writable_blob[wb_idx++] = static_cast<uint8_t>((header >> 8) & 0xFF);
	writable_blob[wb_idx++] = static_cast<uint8_t>((header >> 8) & 0xFF);

	// Add data bytes to the blob
	for (int i = data_byte_size - 1; i >= 0; --i) {
		writable_blob[wb_idx++] = static_cast<uint8_t>((int_value >> (i * 8)) & 0xFF);
	}
	return blob;
}

struct IntTryCastToBit {
	template <class SRC>
	static inline string_t Operation(SRC input, Vector &result) {
		return StringVector::AddStringOrBlob(result, IntToVarInt(input));
	}
};

BoundCastInfo DefaultCasts::ToVarintCastSwitch(BindCastInput &input, const LogicalType &source,
                                               const LogicalType &target) {
	D_ASSERT(target.id() == LogicalTypeId::VARINT);
	// now switch on the result type
	switch (source.id()) {
	case LogicalTypeId::INTEGER:
		return BoundCastInfo(&VectorCastHelpers::StringCast<int32_t, duckdb::IntTryCastToBit>);
	case LogicalTypeId::DOUBLE:
		return TryVectorNullCast;
	default:
		return TryVectorNullCast;
	}
}

BoundCastInfo DefaultCasts::VarintCastSwitch(BindCastInput &input, const LogicalType &source,
                                             const LogicalType &target) {
	// now switch on the result type
	switch (target.id()) {
	default:
		return TryVectorNullCast;
	}
}

} // namespace duckdb
