#include "parquet_statistics.hpp"
#include "parquet_decimal_utils.hpp"
#include "parquet_timestamp.hpp"

#include "duckdb.hpp"
#ifndef DUCKDB_AMALGAMATION
#include "duckdb/common/types/blob.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/storage/statistics/numeric_statistics.hpp"
#include "duckdb/storage/statistics/string_statistics.hpp"
#endif

namespace duckdb {

using duckdb_parquet::format::ConvertedType;
using duckdb_parquet::format::Type;

static unique_ptr<BaseStatistics> CreateNumericStats(const LogicalType &type,
                                                     const duckdb_parquet::format::SchemaElement &schema_ele,
                                                     const duckdb_parquet::format::Statistics &parquet_stats) {
	auto stats = make_unique<NumericStatistics>(type, StatisticsType::LOCAL_STATS);

	// for reasons unknown to science, Parquet defines *both* `min` and `min_value` as well as `max` and
	// `max_value`. All are optional. such elegance.
	if (parquet_stats.__isset.min) {
		stats->min = ParquetStatisticsUtils::ConvertValue(type, schema_ele, parquet_stats.min).CastAs(type);
	} else if (parquet_stats.__isset.min_value) {
		stats->min = ParquetStatisticsUtils::ConvertValue(type, schema_ele, parquet_stats.min_value).CastAs(type);
	} else {
		stats->min = Value(type);
	}
	if (parquet_stats.__isset.max) {
		stats->max = ParquetStatisticsUtils::ConvertValue(type, schema_ele, parquet_stats.max).CastAs(type);
	} else if (parquet_stats.__isset.max_value) {
		stats->max = ParquetStatisticsUtils::ConvertValue(type, schema_ele, parquet_stats.max_value).CastAs(type);
	} else {
		stats->max = Value(type);
	}
	return move(stats);
}

Value ParquetStatisticsUtils::ConvertValue(const LogicalType &type,
                                           const duckdb_parquet::format::SchemaElement &schema_ele,
                                           const std::string &stats) {
	if (stats.empty()) {
		return Value();
	}
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN: {
		if (stats.size() != sizeof(bool)) {
			throw InternalException("Incorrect stats size for type BOOLEAN");
		}
		return Value::BOOLEAN(Load<bool>((data_ptr_t)stats.c_str()));
	}
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
		if (stats.size() != sizeof(uint32_t)) {
			throw InternalException("Incorrect stats size for type UINTEGER");
		}
		return Value::UINTEGER(Load<uint32_t>((data_ptr_t)stats.c_str()));
	case LogicalTypeId::UBIGINT:
		if (stats.size() != sizeof(uint64_t)) {
			throw InternalException("Incorrect stats size for type UBIGINT");
		}
		return Value::UBIGINT(Load<uint64_t>((data_ptr_t)stats.c_str()));
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
		if (stats.size() != sizeof(int32_t)) {
			throw InternalException("Incorrect stats size for type INTEGER");
		}
		return Value::INTEGER(Load<int32_t>((data_ptr_t)stats.c_str()));
	case LogicalTypeId::BIGINT:
		if (stats.size() != sizeof(int64_t)) {
			throw InternalException("Incorrect stats size for type BIGINT");
		}
		return Value::BIGINT(Load<int64_t>((data_ptr_t)stats.c_str()));
	case LogicalTypeId::FLOAT: {
		if (stats.size() != sizeof(float)) {
			throw InternalException("Incorrect stats size for type FLOAT");
		}
		auto val = Load<float>((data_ptr_t)stats.c_str());
		if (!Value::FloatIsFinite(val)) {
			return Value();
		}
		return Value::FLOAT(val);
	}
	case LogicalTypeId::DOUBLE: {
		if (stats.size() != sizeof(double)) {
			throw InternalException("Incorrect stats size for type DOUBLE");
		}
		auto val = Load<double>((data_ptr_t)stats.c_str());
		if (!Value::DoubleIsFinite(val)) {
			return Value();
		}
		return Value::DOUBLE(val);
	}
	case LogicalTypeId::DECIMAL: {
		auto width = DecimalType::GetWidth(type);
		auto scale = DecimalType::GetScale(type);
		switch (schema_ele.type) {
		case Type::INT32: {
			if (stats.size() != sizeof(int32_t)) {
				throw InternalException("Incorrect stats size for type %s", type.ToString());
			}
			return Value::DECIMAL(Load<int32_t>((data_ptr_t)stats.c_str()), width, scale);
		}
		case Type::INT64: {
			if (stats.size() != sizeof(int64_t)) {
				throw InternalException("Incorrect stats size for type %s", type.ToString());
			}
			return Value::DECIMAL(Load<int64_t>((data_ptr_t)stats.c_str()), width, scale);
		}
		case Type::BYTE_ARRAY:
		case Type::FIXED_LEN_BYTE_ARRAY:
			switch (type.InternalType()) {
			case PhysicalType::INT16:
				return Value::DECIMAL(
				    ParquetDecimalUtils::ReadDecimalValue<int16_t>((const_data_ptr_t)stats.c_str(), stats.size()),
				    width, scale);
			case PhysicalType::INT32:
				return Value::DECIMAL(
				    ParquetDecimalUtils::ReadDecimalValue<int32_t>((const_data_ptr_t)stats.c_str(), stats.size()),
				    width, scale);
			case PhysicalType::INT64:
				return Value::DECIMAL(
				    ParquetDecimalUtils::ReadDecimalValue<int64_t>((const_data_ptr_t)stats.c_str(), stats.size()),
				    width, scale);
			case PhysicalType::INT128:
				return Value::DECIMAL(
				    ParquetDecimalUtils::ReadDecimalValue<hugeint_t>((const_data_ptr_t)stats.c_str(), stats.size()),
				    width, scale);
			default:
				throw InternalException("Unsupported internal type for decimal");
			}
		default:
			throw InternalException("Unsupported internal type for decimal?..");
		}
	}
	case LogicalType::VARCHAR:
	case LogicalType::BLOB:
		if (Value::StringIsValid(stats)) {
			return Value(stats);
		} else {
			return Value(Blob::ToString(string_t(stats)));
		}
	case LogicalTypeId::DATE:
		if (stats.size() != sizeof(int32_t)) {
			throw InternalException("Incorrect stats size for type DATE");
		}
		return Value::DATE(date_t(Load<int32_t>((data_ptr_t)stats.c_str())));
	case LogicalTypeId::TIME:
		if (stats.size() != sizeof(int64_t)) {
			throw InternalException("Incorrect stats size for type TIME");
		}
		return Value::TIME(dtime_t(Load<int64_t>((data_ptr_t)stats.c_str())));
	case LogicalTypeId::TIMESTAMP: {
		if (schema_ele.type == Type::INT96) {
			if (stats.size() != sizeof(Int96)) {
				throw InternalException("Incorrect stats size for type TIMESTAMP");
			}
			return Value::TIMESTAMP(ImpalaTimestampToTimestamp(Load<Int96>((data_ptr_t)stats.c_str())));
		} else {
			D_ASSERT(schema_ele.type == Type::INT64);
			if (stats.size() != sizeof(int64_t)) {
				throw InternalException("Incorrect stats size for type TIMESTAMP");
			}
			auto val = Load<int64_t>((data_ptr_t)stats.c_str());
			if (schema_ele.converted_type == duckdb_parquet::format::ConvertedType::TIMESTAMP_MILLIS) {
				return Value::TIMESTAMPMS(timestamp_t(val));
			} else {
				return Value::TIMESTAMP(timestamp_t(val));
			}
		}
	}
	default:
		throw InternalException("Unsupported type for stats %s", type.ToString());
	}
}

unique_ptr<BaseStatistics> ParquetStatisticsUtils::TransformColumnStatistics(const SchemaElement &s_ele,
                                                                             const LogicalType &type,
                                                                             const ColumnChunk &column_chunk) {
	if (!column_chunk.__isset.meta_data || !column_chunk.meta_data.__isset.statistics) {
		// no stats present for row group
		return nullptr;
	}
	auto &parquet_stats = column_chunk.meta_data.statistics;
	unique_ptr<BaseStatistics> row_group_stats;

	switch (type.id()) {
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::DECIMAL:
		row_group_stats = CreateNumericStats(type, s_ele, parquet_stats);
		break;
	case LogicalTypeId::VARCHAR: {
		auto string_stats = make_unique<StringStatistics>(type, StatisticsType::LOCAL_STATS);
		if (parquet_stats.__isset.min) {
			string_stats->Update(parquet_stats.min);
		} else if (parquet_stats.__isset.min_value) {
			string_stats->Update(parquet_stats.min_value);
		} else {
			return nullptr;
		}
		if (parquet_stats.__isset.max) {
			string_stats->Update(parquet_stats.max);
		} else if (parquet_stats.__isset.max_value) {
			string_stats->Update(parquet_stats.max_value);
		} else {
			return nullptr;
		}
		string_stats->has_unicode = true; // we dont know better
		string_stats->max_string_length = NumericLimits<uint32_t>::Maximum();
		row_group_stats = move(string_stats);
		break;
	}
	default:
		// no stats for you
		break;
	} // end of type switch

	// null count is generic
	if (row_group_stats) {
		if (column_chunk.meta_data.type == duckdb_parquet::format::Type::FLOAT ||
		    column_chunk.meta_data.type == duckdb_parquet::format::Type::DOUBLE) {
			// floats/doubles can have infinity, which can become NULL
			row_group_stats->validity_stats = make_unique<ValidityStatistics>(true);
		} else if (parquet_stats.__isset.null_count) {
			row_group_stats->validity_stats = make_unique<ValidityStatistics>(parquet_stats.null_count != 0);
		} else {
			row_group_stats->validity_stats = make_unique<ValidityStatistics>(true);
		}
	} else {
		// if stats are missing from any row group we know squat
		return nullptr;
	}

	return row_group_stats;
}

} // namespace duckdb
