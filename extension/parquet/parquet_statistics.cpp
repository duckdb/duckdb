#include "parquet_statistics.hpp"

#include "duckdb.hpp"
#include "parquet_decimal_utils.hpp"
#include "parquet_timestamp.hpp"
#include "string_column_reader.hpp"
#ifndef DUCKDB_AMALGAMATION
#include "duckdb/common/types/blob.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/value.hpp"
#endif

namespace duckdb {

using duckdb_parquet::format::ConvertedType;
using duckdb_parquet::format::Type;

static unique_ptr<BaseStatistics> CreateNumericStats(const LogicalType &type,
                                                     const duckdb_parquet::format::SchemaElement &schema_ele,
                                                     const duckdb_parquet::format::Statistics &parquet_stats) {
	auto stats = NumericStats::CreateUnknown(type);

	// for reasons unknown to science, Parquet defines *both* `min` and `min_value` as well as `max` and
	// `max_value`. All are optional. such elegance.
	Value min;
	Value max;
	if (parquet_stats.__isset.min) {
		min = ParquetStatisticsUtils::ConvertValue(type, schema_ele, parquet_stats.min).DefaultCastAs(type);
	} else if (parquet_stats.__isset.min_value) {
		min = ParquetStatisticsUtils::ConvertValue(type, schema_ele, parquet_stats.min_value).DefaultCastAs(type);
	} else {
		min = Value(type);
	}
	if (parquet_stats.__isset.max) {
		max = ParquetStatisticsUtils::ConvertValue(type, schema_ele, parquet_stats.max).DefaultCastAs(type);
	} else if (parquet_stats.__isset.max_value) {
		max = ParquetStatisticsUtils::ConvertValue(type, schema_ele, parquet_stats.max_value).DefaultCastAs(type);
	} else {
		max = Value(type);
	}
	NumericStats::SetMin(stats, min);
	NumericStats::SetMax(stats, max);
	return stats.ToUnique();
}

Value ParquetStatisticsUtils::ConvertValue(const LogicalType &type,
                                           const duckdb_parquet::format::SchemaElement &schema_ele,
                                           const std::string &stats) {
	if (stats.empty()) {
		return Value();
	}
	auto stats_data = const_data_ptr_cast(stats.c_str());
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN: {
		if (stats.size() != sizeof(bool)) {
			throw InternalException("Incorrect stats size for type BOOLEAN");
		}
		return Value::BOOLEAN(Load<bool>(stats_data));
	}
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
		if (stats.size() != sizeof(uint32_t)) {
			throw InternalException("Incorrect stats size for type UINTEGER");
		}
		return Value::UINTEGER(Load<uint32_t>(stats_data));
	case LogicalTypeId::UBIGINT:
		if (stats.size() != sizeof(uint64_t)) {
			throw InternalException("Incorrect stats size for type UBIGINT");
		}
		return Value::UBIGINT(Load<uint64_t>(stats_data));
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
		if (stats.size() != sizeof(int32_t)) {
			throw InternalException("Incorrect stats size for type INTEGER");
		}
		return Value::INTEGER(Load<int32_t>(stats_data));
	case LogicalTypeId::BIGINT:
		if (stats.size() != sizeof(int64_t)) {
			throw InternalException("Incorrect stats size for type BIGINT");
		}
		return Value::BIGINT(Load<int64_t>(stats_data));
	case LogicalTypeId::FLOAT: {
		if (stats.size() != sizeof(float)) {
			throw InternalException("Incorrect stats size for type FLOAT");
		}
		auto val = Load<float>(stats_data);
		if (!Value::FloatIsFinite(val)) {
			return Value();
		}
		return Value::FLOAT(val);
	}
	case LogicalTypeId::DOUBLE: {
		if (stats.size() != sizeof(double)) {
			throw InternalException("Incorrect stats size for type DOUBLE");
		}
		auto val = Load<double>(stats_data);
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
			return Value::DECIMAL(Load<int32_t>(stats_data), width, scale);
		}
		case Type::INT64: {
			if (stats.size() != sizeof(int64_t)) {
				throw InternalException("Incorrect stats size for type %s", type.ToString());
			}
			return Value::DECIMAL(Load<int64_t>(stats_data), width, scale);
		}
		case Type::BYTE_ARRAY:
		case Type::FIXED_LEN_BYTE_ARRAY:
			if (stats.size() > GetTypeIdSize(type.InternalType())) {
				throw InternalException("Incorrect stats size for type %s", type.ToString());
			}
			switch (type.InternalType()) {
			case PhysicalType::INT16:
				return Value::DECIMAL(ParquetDecimalUtils::ReadDecimalValue<int16_t>(stats_data, stats.size()), width,
				                      scale);
			case PhysicalType::INT32:
				return Value::DECIMAL(ParquetDecimalUtils::ReadDecimalValue<int32_t>(stats_data, stats.size()), width,
				                      scale);
			case PhysicalType::INT64:
				return Value::DECIMAL(ParquetDecimalUtils::ReadDecimalValue<int64_t>(stats_data, stats.size()), width,
				                      scale);
			case PhysicalType::INT128:
				return Value::DECIMAL(ParquetDecimalUtils::ReadDecimalValue<hugeint_t>(stats_data, stats.size()), width,
				                      scale);
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
		return Value::DATE(date_t(Load<int32_t>(stats_data)));
	case LogicalTypeId::TIME: {
		int64_t val;
		if (stats.size() == sizeof(int32_t)) {
			val = Load<int32_t>(stats_data);
		} else if (stats.size() == sizeof(int64_t)) {
			val = Load<int64_t>(stats_data);
		} else {
			throw InternalException("Incorrect stats size for type TIME");
		}
		if (schema_ele.__isset.logicalType && schema_ele.logicalType.__isset.TIME) {
			// logical type
			if (schema_ele.logicalType.TIME.unit.__isset.MILLIS) {
				return Value::TIME(Time::FromTimeMs(val));
			} else if (schema_ele.logicalType.TIME.unit.__isset.NANOS) {
				return Value::TIME(Time::FromTimeNs(val));
			} else if (schema_ele.logicalType.TIME.unit.__isset.MICROS) {
				return Value::TIME(dtime_t(val));
			} else {
				throw InternalException("Time logicalType is set but unit is not defined");
			}
		}
		if (schema_ele.converted_type == duckdb_parquet::format::ConvertedType::TIME_MILLIS) {
			return Value::TIME(Time::FromTimeMs(val));
		} else {
			return Value::TIME(dtime_t(val));
		}
	}
	case LogicalTypeId::TIME_TZ: {
		int64_t val;
		if (stats.size() == sizeof(int64_t)) {
			val = Load<int64_t>(stats_data);
		} else {
			throw InternalException("Incorrect stats size for type TIMETZ");
		}
		if (schema_ele.__isset.logicalType && schema_ele.logicalType.__isset.TIME) {
			// logical type
			if (schema_ele.logicalType.TIME.unit.__isset.MICROS) {
				return Value::TIMETZ(ParquetIntToTimeTZ(val));
			} else {
				throw InternalException("Time With Time Zone logicalType is set but unit is not defined");
			}
		}
		return Value::TIMETZ(ParquetIntToTimeTZ(val));
	}
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ: {
		if (schema_ele.type == Type::INT96) {
			if (stats.size() != sizeof(Int96)) {
				throw InternalException("Incorrect stats size for type TIMESTAMP");
			}
			return Value::TIMESTAMP(ImpalaTimestampToTimestamp(Load<Int96>(stats_data)));
		} else {
			D_ASSERT(schema_ele.type == Type::INT64);
			if (stats.size() != sizeof(int64_t)) {
				throw InternalException("Incorrect stats size for type TIMESTAMP");
			}
			auto val = Load<int64_t>(stats_data);
			if (schema_ele.__isset.logicalType && schema_ele.logicalType.__isset.TIMESTAMP) {
				// logical type
				if (schema_ele.logicalType.TIMESTAMP.unit.__isset.MILLIS) {
					return Value::TIMESTAMPMS(timestamp_t(val));
				} else if (schema_ele.logicalType.TIMESTAMP.unit.__isset.NANOS) {
					return Value::TIMESTAMPNS(timestamp_t(val));
				} else if (schema_ele.logicalType.TIMESTAMP.unit.__isset.MICROS) {
					return Value::TIMESTAMP(timestamp_t(val));
				} else {
					throw InternalException("Timestamp logicalType is set but unit is not defined");
				}
			}
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
		auto string_stats = StringStats::CreateEmpty(type);
		if (parquet_stats.__isset.min) {
			StringColumnReader::VerifyString(parquet_stats.min.c_str(), parquet_stats.min.size(), true);
			StringStats::Update(string_stats, parquet_stats.min);
		} else if (parquet_stats.__isset.min_value) {
			StringColumnReader::VerifyString(parquet_stats.min_value.c_str(), parquet_stats.min_value.size(), true);
			StringStats::Update(string_stats, parquet_stats.min_value);
		} else {
			return nullptr;
		}
		if (parquet_stats.__isset.max) {
			StringColumnReader::VerifyString(parquet_stats.max.c_str(), parquet_stats.max.size(), true);
			StringStats::Update(string_stats, parquet_stats.max);
		} else if (parquet_stats.__isset.max_value) {
			StringColumnReader::VerifyString(parquet_stats.max_value.c_str(), parquet_stats.max_value.size(), true);
			StringStats::Update(string_stats, parquet_stats.max_value);
		} else {
			return nullptr;
		}
		StringStats::SetContainsUnicode(string_stats);
		StringStats::ResetMaxStringLength(string_stats);
		row_group_stats = string_stats.ToUnique();
		break;
	}
	default:
		// no stats for you
		break;
	} // end of type switch

	// null count is generic
	if (!row_group_stats) {
		// if stats are missing from any row group we know squat
		return nullptr;
	}
	row_group_stats->Set(StatsInfo::CAN_HAVE_NULL_AND_VALID_VALUES);
	if (parquet_stats.__isset.null_count && parquet_stats.null_count == 0) {
		row_group_stats->Set(StatsInfo::CANNOT_HAVE_NULL_VALUES);
	}
	return row_group_stats;
}

} // namespace duckdb
