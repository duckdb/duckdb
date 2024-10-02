#include "parquet_statistics.hpp"

#include "duckdb.hpp"
#include "parquet_decimal_utils.hpp"
#include "parquet_timestamp.hpp"
#include "string_column_reader.hpp"
#include "struct_column_reader.hpp"
#ifndef DUCKDB_AMALGAMATION
#include "duckdb/common/types/blob.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/storage/statistics/struct_stats.hpp"
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
	if (parquet_stats.__isset.min_value) {
		min = ParquetStatisticsUtils::ConvertValue(type, schema_ele, parquet_stats.min_value).DefaultCastAs(type);
	} else if (parquet_stats.__isset.min) {
		min = ParquetStatisticsUtils::ConvertValue(type, schema_ele, parquet_stats.min).DefaultCastAs(type);
	} else {
		min = Value(type);
	}
	if (parquet_stats.__isset.max_value) {
		max = ParquetStatisticsUtils::ConvertValue(type, schema_ele, parquet_stats.max_value).DefaultCastAs(type);
	} else if (parquet_stats.__isset.max) {
		max = ParquetStatisticsUtils::ConvertValue(type, schema_ele, parquet_stats.max).DefaultCastAs(type);
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
		switch (schema_ele.type) {
		case Type::FIXED_LEN_BYTE_ARRAY:
		case Type::BYTE_ARRAY:
			// decimals cast to double
			return Value::DOUBLE(ParquetDecimalUtils::ReadDecimalValue<double>(stats_data, stats.size(), schema_ele));
		default:
			break;
		}
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
			switch (type.InternalType()) {
			case PhysicalType::INT16:
				return Value::DECIMAL(
				    ParquetDecimalUtils::ReadDecimalValue<int16_t>(stats_data, stats.size(), schema_ele), width, scale);
			case PhysicalType::INT32:
				return Value::DECIMAL(
				    ParquetDecimalUtils::ReadDecimalValue<int32_t>(stats_data, stats.size(), schema_ele), width, scale);
			case PhysicalType::INT64:
				return Value::DECIMAL(
				    ParquetDecimalUtils::ReadDecimalValue<int64_t>(stats_data, stats.size(), schema_ele), width, scale);
			case PhysicalType::INT128:
				return Value::DECIMAL(
				    ParquetDecimalUtils::ReadDecimalValue<hugeint_t>(stats_data, stats.size(), schema_ele), width,
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
		if (stats.size() == sizeof(int32_t)) {
			val = Load<int32_t>(stats_data);
		} else if (stats.size() == sizeof(int64_t)) {
			val = Load<int64_t>(stats_data);
		} else {
			throw InternalException("Incorrect stats size for type TIMETZ");
		}
		if (schema_ele.__isset.logicalType && schema_ele.logicalType.__isset.TIME) {
			// logical type
			if (schema_ele.logicalType.TIME.unit.__isset.MILLIS) {
				return Value::TIMETZ(ParquetIntToTimeMsTZ(NumericCast<int32_t>(val)));
			} else if (schema_ele.logicalType.TIME.unit.__isset.MICROS) {
				return Value::TIMETZ(ParquetIntToTimeTZ(val));
			} else if (schema_ele.logicalType.TIME.unit.__isset.NANOS) {
				return Value::TIMETZ(ParquetIntToTimeNsTZ(val));
			} else {
				throw InternalException("Time With Time Zone logicalType is set but unit is not defined");
			}
		}
		return Value::TIMETZ(ParquetIntToTimeTZ(val));
	}
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ: {
		timestamp_t timestamp_value;
		if (schema_ele.type == Type::INT96) {
			if (stats.size() != sizeof(Int96)) {
				throw InternalException("Incorrect stats size for type TIMESTAMP");
			}
			timestamp_value = ImpalaTimestampToTimestamp(Load<Int96>(stats_data));
		} else {
			D_ASSERT(schema_ele.type == Type::INT64);
			if (stats.size() != sizeof(int64_t)) {
				throw InternalException("Incorrect stats size for type TIMESTAMP");
			}
			auto val = Load<int64_t>(stats_data);
			if (schema_ele.__isset.logicalType && schema_ele.logicalType.__isset.TIMESTAMP) {
				// logical type
				if (schema_ele.logicalType.TIMESTAMP.unit.__isset.MILLIS) {
					timestamp_value = Timestamp::FromEpochMs(val);
				} else if (schema_ele.logicalType.TIMESTAMP.unit.__isset.NANOS) {
					timestamp_value = Timestamp::FromEpochNanoSeconds(val);
				} else if (schema_ele.logicalType.TIMESTAMP.unit.__isset.MICROS) {
					timestamp_value = timestamp_t(val);
				} else {
					throw InternalException("Timestamp logicalType is set but unit is not defined");
				}
			} else if (schema_ele.converted_type == duckdb_parquet::format::ConvertedType::TIMESTAMP_MILLIS) {
				timestamp_value = Timestamp::FromEpochMs(val);
			} else {
				timestamp_value = timestamp_t(val);
			}
		}
		if (type.id() == LogicalTypeId::TIMESTAMP_TZ) {
			return Value::TIMESTAMPTZ(timestamp_value);
		} else {
			return Value::TIMESTAMP(timestamp_value);
		}
	}
	case LogicalTypeId::TIMESTAMP_NS: {
		timestamp_ns_t timestamp_value;
		if (schema_ele.type == Type::INT96) {
			if (stats.size() != sizeof(Int96)) {
				throw InternalException("Incorrect stats size for type TIMESTAMP_NS");
			}
			timestamp_value = ImpalaTimestampToTimestampNS(Load<Int96>(stats_data));
		} else {
			D_ASSERT(schema_ele.type == Type::INT64);
			if (stats.size() != sizeof(int64_t)) {
				throw InternalException("Incorrect stats size for type TIMESTAMP_NS");
			}
			auto val = Load<int64_t>(stats_data);
			if (schema_ele.__isset.logicalType && schema_ele.logicalType.__isset.TIMESTAMP) {
				// logical type
				if (schema_ele.logicalType.TIMESTAMP.unit.__isset.MILLIS) {
					timestamp_value = ParquetTimestampMsToTimestampNs(val);
				} else if (schema_ele.logicalType.TIMESTAMP.unit.__isset.NANOS) {
					timestamp_value = ParquetTimestampNsToTimestampNs(val);
				} else if (schema_ele.logicalType.TIMESTAMP.unit.__isset.MICROS) {
					timestamp_value = ParquetTimestampUsToTimestampNs(val);
				} else {
					throw InternalException("Timestamp (NS) logicalType is set but unit is unknown");
				}
			} else if (schema_ele.converted_type == duckdb_parquet::format::ConvertedType::TIMESTAMP_MILLIS) {
				timestamp_value = ParquetTimestampMsToTimestampNs(val);
			} else {
				timestamp_value = ParquetTimestampUsToTimestampNs(val);
			}
		}
		return Value::TIMESTAMPNS(timestamp_value);
	}
	default:
		throw InternalException("Unsupported type for stats %s", type.ToString());
	}
}

unique_ptr<BaseStatistics> ParquetStatisticsUtils::TransformColumnStatistics(const ColumnReader &reader,
                                                                             const vector<ColumnChunk> &columns) {

	// Not supported types
	if (reader.Type().id() == LogicalTypeId::ARRAY || reader.Type().id() == LogicalTypeId::MAP ||
	    reader.Type().id() == LogicalTypeId::LIST) {
		return nullptr;
	}

	unique_ptr<BaseStatistics> row_group_stats;

	// Structs are handled differently (they dont have stats)
	if (reader.Type().id() == LogicalTypeId::STRUCT) {
		auto struct_stats = StructStats::CreateUnknown(reader.Type());
		auto &struct_reader = reader.Cast<StructColumnReader>();
		// Recurse into child readers
		for (idx_t i = 0; i < struct_reader.child_readers.size(); i++) {
			auto &child_reader = *struct_reader.child_readers[i];
			auto child_stats = ParquetStatisticsUtils::TransformColumnStatistics(child_reader, columns);
			StructStats::SetChildStats(struct_stats, i, std::move(child_stats));
		}
		row_group_stats = struct_stats.ToUnique();

		// null count is generic
		if (row_group_stats) {
			row_group_stats->Set(StatsInfo::CAN_HAVE_NULL_AND_VALID_VALUES);
		}
		return row_group_stats;
	}

	// Otherwise, its a standard column with stats

	auto &column_chunk = columns[reader.FileIdx()];
	if (!column_chunk.__isset.meta_data || !column_chunk.meta_data.__isset.statistics) {
		// no stats present for row group
		return nullptr;
	}
	auto &parquet_stats = column_chunk.meta_data.statistics;

	auto &type = reader.Type();
	auto &s_ele = reader.Schema();

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
	case LogicalTypeId::TIME_TZ:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::DECIMAL:
		row_group_stats = CreateNumericStats(type, s_ele, parquet_stats);
		break;
	case LogicalTypeId::VARCHAR: {
		auto string_stats = StringStats::CreateEmpty(type);
		if (parquet_stats.__isset.min_value) {
			StringColumnReader::VerifyString(parquet_stats.min_value.c_str(), parquet_stats.min_value.size(), true);
			StringStats::Update(string_stats, parquet_stats.min_value);
		} else if (parquet_stats.__isset.min) {
			StringColumnReader::VerifyString(parquet_stats.min.c_str(), parquet_stats.min.size(), true);
			StringStats::Update(string_stats, parquet_stats.min);
		} else {
			return nullptr;
		}
		if (parquet_stats.__isset.max_value) {
			StringColumnReader::VerifyString(parquet_stats.max_value.c_str(), parquet_stats.max_value.size(), true);
			StringStats::Update(string_stats, parquet_stats.max_value);
		} else if (parquet_stats.__isset.max) {
			StringColumnReader::VerifyString(parquet_stats.max.c_str(), parquet_stats.max.size(), true);
			StringStats::Update(string_stats, parquet_stats.max);
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
	if (row_group_stats) {
		row_group_stats->Set(StatsInfo::CAN_HAVE_NULL_AND_VALID_VALUES);
		if (parquet_stats.__isset.null_count && parquet_stats.null_count == 0) {
			row_group_stats->Set(StatsInfo::CANNOT_HAVE_NULL_VALUES);
		}
	}
	return row_group_stats;
}

} // namespace duckdb
