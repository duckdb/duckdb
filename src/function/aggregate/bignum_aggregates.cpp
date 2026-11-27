#include "duckdb/function/aggregate/bignum_aggregates.hpp"

namespace duckdb {

void BignumAggregates::WriteBignum(const BignumIntermediate &value, VectorWriter<string_t> &writer) {
	// trimmed like BignumIntermediate::ToBignum, the intermediate itself is only read
	auto start = value.GetStartDataPos();
	uint32_t size = value.size - start;
	if (size == 0) {
		// the value is all sign-extension bytes: keep one of them
		start = 0;
		size = 1;
	}
	auto &blob = writer.WriteEmptyString(Bignum::BIGNUM_HEADER_SIZE + size);
	auto ptr = blob.GetDataWriteable();
	Bignum::SetHeader(ptr, size, value.is_negative);
	memcpy(ptr + Bignum::BIGNUM_HEADER_SIZE, value.data + start, size);
	blob.Finalize();
}

BignumIntermediate BignumAggregates::ImportBignum(string_t blob, ArenaAllocator &allocator) {
	const auto size = blob.GetSize();
	auto header = const_data_ptr_cast(blob.GetData());
	if (size < Bignum::BIGNUM_HEADER_SIZE + 1) {
		throw InvalidInputException("Invalid BIGNUM state - the blob is shorter than a header and a byte");
	}
	// the header holds the data size, inverted for negative values (see Bignum::SetHeader)
	uint32_t data_size = (uint32_t(header[0]) << 16) | (uint32_t(header[1]) << 8) | uint32_t(header[2]);
	if ((header[0] & 0x80) == 0) {
		data_size = ~data_size;
	}
	data_size &= 0x7FFFFF;
	if (size != data_size + Bignum::BIGNUM_HEADER_SIZE) {
		throw InvalidInputException("Invalid BIGNUM state - the header does not match the size of the blob");
	}
	auto ptr = allocator.Allocate(size);
	memcpy(ptr, blob.GetData(), size);
	return BignumIntermediate(ptr, size);
}

void BignumAggregates::WriteAvgState(const BignumAvgState &state, VectorWriter<uint64_t> &count_writer,
                                     VectorWriter<string_t> &value_writer) {
	// the state is only read: it is exported while other threads may still read it
	count_writer.WriteValue(state.count);
	if (state.count == 0) {
		value_writer.WriteNull();
	} else {
		WriteBignum(state.value, value_writer);
	}
}

void BignumAggregates::ImportAvgState(BignumAvgState &state, const VectorIterator<uint64_t>::ValueEntry &count,
                                      const VectorIterator<string_t>::ValueEntry &value, ArenaAllocator &allocator,
                                      const char *function_name, const char *value_field) {
	if (!count.IsValid() || count.GetValue() == 0) {
		// an empty state
		return;
	}
	if (!value.IsValid()) {
		throw InvalidInputException("Invalid %s state - the %s cannot be NULL when the count is not 0", function_name,
		                            value_field);
	}
	state.count = count.GetValue();
	state.value = ImportBignum(value.GetValue(), allocator);
}

} // namespace duckdb
