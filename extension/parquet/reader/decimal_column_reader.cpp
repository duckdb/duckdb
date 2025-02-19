#include "reader/decimal_column_reader.hpp"

namespace duckdb {

template <bool FIXED>
unique_ptr<ColumnReader> CreateDecimalReaderInternal(ParquetReader &reader, const ParquetColumnSchema &schema) {
	switch (schema.type.InternalType()) {
	case PhysicalType::INT16:
		return make_uniq<DecimalColumnReader<int16_t, FIXED>>(reader, schema);
	case PhysicalType::INT32:
		return make_uniq<DecimalColumnReader<int32_t, FIXED>>(reader, schema);
	case PhysicalType::INT64:
		return make_uniq<DecimalColumnReader<int64_t, FIXED>>(reader, schema);
	case PhysicalType::INT128:
		return make_uniq<DecimalColumnReader<hugeint_t, FIXED>>(reader, schema);
	case PhysicalType::DOUBLE:
		return make_uniq<DecimalColumnReader<double, FIXED>>(reader, schema);
	default:
		throw InternalException("Unrecognized type for Decimal");
	}
}

template <>
double ParquetDecimalUtils::ReadDecimalValue(const_data_ptr_t pointer, idx_t size,
                                             const ParquetColumnSchema &schema_ele) {
	double res = 0;
	bool positive = (*pointer & 0x80) == 0;
	for (idx_t i = 0; i < size; i += 8) {
		auto byte_size = MinValue<idx_t>(sizeof(uint64_t), size - i);
		uint64_t input = 0;
		auto res_ptr = reinterpret_cast<uint8_t *>(&input);
		for (idx_t k = 0; k < byte_size; k++) {
			auto byte = pointer[i + k];
			res_ptr[sizeof(uint64_t) - k - 1] = positive ? byte : byte ^ 0xFF;
		}
		res *= double(NumericLimits<uint64_t>::Maximum()) + 1;
		res += static_cast<double>(input);
	}
	if (!positive) {
		res += 1;
		res /= pow(10, schema_ele.type_scale);
		return -res;
	}
	res /= pow(10, schema_ele.type_scale);
	return res;
}

unique_ptr<ColumnReader> ParquetDecimalUtils::CreateReader(ParquetReader &reader, const ParquetColumnSchema &schema) {
	if (schema.type_length > 0) {
		return CreateDecimalReaderInternal<true>(reader, schema);
	} else {
		return CreateDecimalReaderInternal<false>(reader, schema);
	}
}

} // namespace duckdb
