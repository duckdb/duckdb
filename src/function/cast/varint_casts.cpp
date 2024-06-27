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
		int_value = ~int_value;
	}

	// Determine the number of data bytes
	uint64_t abs_value = std::abs(int_value);
	uint32_t data_byte_size = (abs_value == 0) ? 1 : static_cast<int>(std::ceil(std::log2(abs_value + 1) / 8.0));

	// Create the header
	uint32_t header = data_byte_size;
	if (!is_negative) {
		header = ~header;
		// header |= (1 << 23); // Set the sign bit for positive numbers
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

// Function to convert VARINT blob to a VARCHAR
// FIXME: This should probably use a double
string_t VarIntToVarchar(string_t &blob) {
	if (blob.GetSize() < 4) {
		throw std::invalid_argument("Invalid blob size.");
	}
	auto blob_ptr = blob.GetData();

	// Extract the header
	uint32_t header = (blob_ptr[0] << 16) | (blob_ptr[1] << 8) | blob_ptr[2];

	// Determine the number of data bytes
	int data_byte_size = blob.GetSize() - 3;

	// Determine if the number is negative
	bool is_negative = (header & (1 << 23)) == 0;

	// Extract the data bytes
	int64_t int_value = 0;
	for (int i = 0; i < data_byte_size; ++i) {
		int_value = (int_value << 8) | blob_ptr[3 + i];
	}

	// If negative, convert from two's complement
	if (is_negative) {
		int_value = ~int_value;

		// int_value = -((~int_value + 1) & ((1LL << (data_byte_size * 8)) - 1));
	}

	return std::to_string(int_value);
}

struct IntTryCastToVarInt {
	template <class SRC>
	static inline string_t Operation(SRC input, Vector &result) {
		return StringVector::AddStringOrBlob(result, IntToVarInt(input));
	}
};

struct VarIntTryCastToVarchar {
	template <class SRC>
	static inline string_t Operation(SRC input, Vector &result) {
		return StringVector::AddStringOrBlob(result, VarIntToVarchar(input));
	}
};

BoundCastInfo DefaultCasts::ToVarintCastSwitch(BindCastInput &input, const LogicalType &source,
                                               const LogicalType &target) {
	D_ASSERT(target.id() == LogicalTypeId::VARINT);
	// now switch on the result type
	switch (source.id()) {
	case LogicalTypeId::INTEGER:
		return BoundCastInfo(&VectorCastHelpers::StringCast<int32_t, duckdb::IntTryCastToVarInt>);
	case LogicalTypeId::DOUBLE:
		return TryVectorNullCast;
	default:
		return TryVectorNullCast;
	}
}

BoundCastInfo DefaultCasts::VarintCastSwitch(BindCastInput &input, const LogicalType &source,
                                             const LogicalType &target) {
	D_ASSERT(source.id() == LogicalTypeId::VARINT);
	// now switch on the result type
	switch (target.id()) {
	case LogicalTypeId::VARCHAR:
		return BoundCastInfo(&VectorCastHelpers::StringCast<string_t, duckdb::VarIntTryCastToVarchar>);
	default:
		return TryVectorNullCast;
	}
}

} // namespace duckdb
