#include "decimal_column_reader.hpp"

namespace duckdb {

template <bool FIXED>
unique_ptr<ColumnReader> CreateDecimalReaderInternal(ParquetReader &reader, const LogicalType &type_p,
															const SchemaElement &schema_p, idx_t file_idx_p,
															idx_t max_define, idx_t max_repeat) {
	switch (type_p.InternalType()) {
	case PhysicalType::INT16:
		return make_uniq<DecimalColumnReader<int16_t, FIXED>>(reader, type_p, schema_p, file_idx_p, max_define,
																	 max_repeat);
	case PhysicalType::INT32:
		return make_uniq<DecimalColumnReader<int32_t, FIXED>>(reader, type_p, schema_p, file_idx_p, max_define,
																	 max_repeat);
	case PhysicalType::INT64:
		return make_uniq<DecimalColumnReader<int64_t, FIXED>>(reader, type_p, schema_p, file_idx_p, max_define,
																	 max_repeat);
	case PhysicalType::INT128:
		return make_uniq<DecimalColumnReader<hugeint_t, FIXED>>(reader, type_p, schema_p, file_idx_p, max_define,
																	   max_repeat);
	case PhysicalType::DOUBLE:
		return make_uniq<DecimalColumnReader<double, FIXED>>(reader, type_p, schema_p, file_idx_p, max_define,
																	max_repeat);
	default:
		throw InternalException("Unrecognized type for Decimal");
	}
}

template <>
double ParquetDecimalUtils::ReadDecimalValue(const_data_ptr_t pointer, idx_t size,
                                             const duckdb_parquet::SchemaElement &schema_ele) {
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
		res /= pow(10, schema_ele.scale);
		return -res;
	}
	res /= pow(10, schema_ele.scale);
	return res;
}

unique_ptr<ColumnReader> ParquetDecimalUtils::CreateReader(ParquetReader &reader, const LogicalType &type_p,
                                                           const SchemaElement &schema_p, idx_t file_idx_p,
                                                           idx_t max_define, idx_t max_repeat) {
	if (schema_p.__isset.type_length) {
		return CreateDecimalReaderInternal<true>(reader, type_p, schema_p, file_idx_p, max_define, max_repeat);
	} else {
		return CreateDecimalReaderInternal<false>(reader, type_p, schema_p, file_idx_p, max_define, max_repeat);
	}
}

}
