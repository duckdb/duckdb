#include "reader/decimal_column_reader.hpp"
#include "duckdb/common/bignum.hpp"
#include "duckdb/common/types/bignum.hpp"
#include "duckdb/common/types/decimal_helper.hpp"

#include <memory>
#include <cmath>

#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "parquet_types.h"

namespace duckdb {
class ParquetReader;
struct hugeint_t;

[[noreturn]] void ParquetDecimalUtils::ThrowInvalidDecimalEncoding() {
	throw DataCorruptionException("Invalid decimal encoding in Parquet file");
}

template <bool FIXED>
static unique_ptr<ColumnReader> CreateDecimalReaderInternal(const ParquetReader &reader,
                                                            const ParquetColumnSchema &schema) {
	switch (schema.type.InternalType()) {
	case PhysicalType::INT16:
		return make_uniq<DecimalColumnReader<int16_t, FIXED>>(reader, schema);
	case PhysicalType::INT32:
		return make_uniq<DecimalColumnReader<int32_t, FIXED>>(reader, schema);
	case PhysicalType::INT64:
		return make_uniq<DecimalColumnReader<int64_t, FIXED>>(reader, schema);
	case PhysicalType::INT128:
		return make_uniq<DecimalColumnReader<hugeint_t, FIXED>>(reader, schema);
	case PhysicalType::VARCHAR:
		// wide decimal stored as a BIGNUM blob
		return make_uniq<WideDecimalColumnReader>(reader, schema);
	default:
		throw InternalException("Unrecognized type for Decimal");
	}
}

unique_ptr<ColumnReader> ParquetDecimalUtils::CreateReader(const ParquetReader &reader,
                                                           const ParquetColumnSchema &schema) {
	if (schema.parquet_type == Type::FIXED_LEN_BYTE_ARRAY) {
		return CreateDecimalReaderInternal<true>(reader, schema);
	} else {
		return CreateDecimalReaderInternal<false>(reader, schema);
	}
}

string ParquetDecimalUtils::ReadWideDecimalValue(const_data_ptr_t pointer, idx_t size) {
	// Parquet decimals are big-endian two's complement
	return DecimalHelper<bignum_t>::FromTwosComplement(pointer, size, true);
}

idx_t ParquetDecimalUtils::WideDecimalByteLength(uint32_t width) {
	// a decimal of "width" digits needs log2(10^width) bits plus a sign bit
	return static_cast<idx_t>(std::ceil((static_cast<double>(width) * 3.3219280948873626 + 1) / 8.0));
}

void ParquetDecimalUtils::WriteWideDecimal(const bignum_t &value, idx_t byte_length, data_ptr_t result) {
	DecimalHelper<bignum_t>::ToTwosComplement(value, result, byte_length, true);
}

} // namespace duckdb
