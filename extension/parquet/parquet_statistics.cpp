#include "parquet_statistics.hpp"

#include "duckdb.hpp"
#include "parquet_decimal_utils.hpp"
#include "parquet_timestamp.hpp"
#include "string_column_reader.hpp"
#include "struct_column_reader.hpp"
#include "zstd/common/xxhash.hpp"

#ifndef DUCKDB_AMALGAMATION
#include "duckdb/common/types/blob.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/storage/statistics/struct_stats.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#endif

namespace duckdb {

using duckdb_parquet::ConvertedType;
using duckdb_parquet::Type;

static unique_ptr<BaseStatistics> CreateNumericStats(const LogicalType &type,
                                                     const duckdb_parquet::SchemaElement &schema_ele,
                                                     const duckdb_parquet::Statistics &parquet_stats) {
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

Value ParquetStatisticsUtils::ConvertValue(const LogicalType &type, const duckdb_parquet::SchemaElement &schema_ele,
                                           const std::string &stats) {
	auto stats_data = const_data_ptr_cast(stats.c_str());
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN: {
		if (stats.size() != sizeof(bool)) {
			throw InvalidInputException("Incorrect stats size for type BOOLEAN");
		}
		return Value::BOOLEAN(Load<bool>(stats_data));
	}
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
		if (stats.size() != sizeof(uint32_t)) {
			throw InvalidInputException("Incorrect stats size for type UINTEGER");
		}
		return Value::UINTEGER(Load<uint32_t>(stats_data));
	case LogicalTypeId::UBIGINT:
		if (stats.size() != sizeof(uint64_t)) {
			throw InvalidInputException("Incorrect stats size for type UBIGINT");
		}
		return Value::UBIGINT(Load<uint64_t>(stats_data));
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
		if (stats.size() != sizeof(int32_t)) {
			throw InvalidInputException("Incorrect stats size for type INTEGER");
		}
		return Value::INTEGER(Load<int32_t>(stats_data));
	case LogicalTypeId::BIGINT:
		if (stats.size() != sizeof(int64_t)) {
			throw InvalidInputException("Incorrect stats size for type BIGINT");
		}
		return Value::BIGINT(Load<int64_t>(stats_data));
	case LogicalTypeId::FLOAT: {
		if (stats.size() != sizeof(float)) {
			throw InvalidInputException("Incorrect stats size for type FLOAT");
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
			throw InvalidInputException("Incorrect stats size for type DOUBLE");
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
				throw InvalidInputException("Incorrect stats size for type %s", type.ToString());
			}
			return Value::DECIMAL(Load<int32_t>(stats_data), width, scale);
		}
		case Type::INT64: {
			if (stats.size() != sizeof(int64_t)) {
				throw InvalidInputException("Incorrect stats size for type %s", type.ToString());
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
				throw InvalidInputException("Unsupported internal type for decimal");
			}
		default:
			throw InternalException("Unsupported internal type for decimal?..");
		}
	}
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::BLOB:
		if (type.id() == LogicalTypeId::BLOB || !Value::StringIsValid(stats)) {
			return Value(Blob::ToString(string_t(stats)));
		}
		return Value(stats);
	case LogicalTypeId::DATE:
		if (stats.size() != sizeof(int32_t)) {
			throw InvalidInputException("Incorrect stats size for type DATE");
		}
		return Value::DATE(date_t(Load<int32_t>(stats_data)));
	case LogicalTypeId::TIME: {
		int64_t val;
		if (stats.size() == sizeof(int32_t)) {
			val = Load<int32_t>(stats_data);
		} else if (stats.size() == sizeof(int64_t)) {
			val = Load<int64_t>(stats_data);
		} else {
			throw InvalidInputException("Incorrect stats size for type TIME");
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
		if (schema_ele.converted_type == duckdb_parquet::ConvertedType::TIME_MILLIS) {
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
			throw InvalidInputException("Incorrect stats size for type TIMETZ");
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
				throw InvalidInputException("Incorrect stats size for type TIMESTAMP");
			}
			timestamp_value = ImpalaTimestampToTimestamp(Load<Int96>(stats_data));
		} else {
			D_ASSERT(schema_ele.type == Type::INT64);
			if (stats.size() != sizeof(int64_t)) {
				throw InvalidInputException("Incorrect stats size for type TIMESTAMP");
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
			} else if (schema_ele.converted_type == duckdb_parquet::ConvertedType::TIMESTAMP_MILLIS) {
				timestamp_value = Timestamp::FromEpochMs(val);
			} else {
				timestamp_value = timestamp_t(val);
			}
		}
		if (type.id() == LogicalTypeId::TIMESTAMP_TZ) {
			return Value::TIMESTAMPTZ(timestamp_tz_t(timestamp_value));
		}
		return Value::TIMESTAMP(timestamp_value);
	}
	case LogicalTypeId::TIMESTAMP_NS: {
		timestamp_ns_t timestamp_value;
		if (schema_ele.type == Type::INT96) {
			if (stats.size() != sizeof(Int96)) {
				throw InvalidInputException("Incorrect stats size for type TIMESTAMP_NS");
			}
			timestamp_value = ImpalaTimestampToTimestampNS(Load<Int96>(stats_data));
		} else {
			D_ASSERT(schema_ele.type == Type::INT64);
			if (stats.size() != sizeof(int64_t)) {
				throw InvalidInputException("Incorrect stats size for type TIMESTAMP_NS");
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
			} else if (schema_ele.converted_type == duckdb_parquet::ConvertedType::TIMESTAMP_MILLIS) {
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
			if (!struct_reader.child_readers[i]) {
				continue;
			}
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

static bool HasFilterConstants(const TableFilter &duckdb_filter) {
	switch (duckdb_filter.filter_type) {
	case TableFilterType::CONSTANT_COMPARISON: {
		auto &constant_filter = duckdb_filter.Cast<ConstantFilter>();
		return (constant_filter.comparison_type == ExpressionType::COMPARE_EQUAL && !constant_filter.constant.IsNull());
	}
	case TableFilterType::CONJUNCTION_AND: {
		auto &conjunction_and_filter = duckdb_filter.Cast<ConjunctionAndFilter>();
		bool child_has_constant = false;
		for (auto &child_filter : conjunction_and_filter.child_filters) {
			child_has_constant |= HasFilterConstants(*child_filter);
		}
		return child_has_constant;
	}
	case TableFilterType::CONJUNCTION_OR: {
		auto &conjunction_or_filter = duckdb_filter.Cast<ConjunctionOrFilter>();
		bool child_has_constant = false;
		for (auto &child_filter : conjunction_or_filter.child_filters) {
			child_has_constant |= HasFilterConstants(*child_filter);
		}
		return child_has_constant;
	}
	default:
		return false;
	}
}

template <class T>
uint64_t ValueXH64FixedWidth(const Value &constant) {
	T val = constant.GetValue<T>();
	return duckdb_zstd::XXH64(&val, sizeof(val), 0);
}

// TODO we can only this if the parquet representation of the type exactly matches the duckdb rep!
// TODO TEST THIS!
// TODO perhaps we can re-use some writer infra here
static uint64_t ValueXXH64(const Value &constant) {
	switch (constant.type().InternalType()) {
	case PhysicalType::UINT8:
		return ValueXH64FixedWidth<int32_t>(constant);
	case PhysicalType::INT8:
		return ValueXH64FixedWidth<int32_t>(constant);
	case PhysicalType::UINT16:
		return ValueXH64FixedWidth<int32_t>(constant);
	case PhysicalType::INT16:
		return ValueXH64FixedWidth<int32_t>(constant);
	case PhysicalType::UINT32:
		return ValueXH64FixedWidth<uint32_t>(constant);
	case PhysicalType::INT32:
		return ValueXH64FixedWidth<int32_t>(constant);
	case PhysicalType::UINT64:
		return ValueXH64FixedWidth<uint64_t>(constant);
	case PhysicalType::INT64:
		return ValueXH64FixedWidth<int64_t>(constant);
	case PhysicalType::FLOAT:
		return ValueXH64FixedWidth<float>(constant);
	case PhysicalType::DOUBLE:
		return ValueXH64FixedWidth<double>(constant);
	case PhysicalType::VARCHAR: {
		auto val = constant.GetValue<string>();
		return duckdb_zstd::XXH64(val.c_str(), val.length(), 0);
	}
	default:
		return 0;
	}
}

static bool ApplyBloomFilter(const TableFilter &duckdb_filter, ParquetBloomFilter &bloom_filter) {
	switch (duckdb_filter.filter_type) {
	case TableFilterType::CONSTANT_COMPARISON: {
		auto &constant_filter = duckdb_filter.Cast<ConstantFilter>();
		D_ASSERT(constant_filter.comparison_type == ExpressionType::COMPARE_EQUAL);
		D_ASSERT(!constant_filter.constant.IsNull());
		auto hash = ValueXXH64(constant_filter.constant);
		return hash > 0 && !bloom_filter.FilterCheck(hash);
	}
	case TableFilterType::CONJUNCTION_AND: {
		auto &conjunction_and_filter = duckdb_filter.Cast<ConjunctionAndFilter>();
		bool any_children_true = false;
		for (auto &child_filter : conjunction_and_filter.child_filters) {
			any_children_true |= ApplyBloomFilter(*child_filter, bloom_filter);
		}
		return any_children_true;
	}
	case TableFilterType::CONJUNCTION_OR: {
		auto &conjunction_or_filter = duckdb_filter.Cast<ConjunctionOrFilter>();
		bool all_children_true = true;
		for (auto &child_filter : conjunction_or_filter.child_filters) {
			all_children_true &= ApplyBloomFilter(*child_filter, bloom_filter);
		}
		return all_children_true;
	}
	default:
		return false;
	}
}

bool ParquetStatisticsUtils::BloomFilterSupported(const LogicalTypeId &type_id) {
	switch (type_id) {
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::BLOB:
		return true;
	default:
		return false;
	}
}

bool ParquetStatisticsUtils::BloomFilterExcludes(const TableFilter &duckdb_filter,
                                                 const duckdb_parquet::ColumnMetaData &column_meta_data,
                                                 TProtocol &file_proto, Allocator &allocator) {
	if (!HasFilterConstants(duckdb_filter) || !column_meta_data.__isset.bloom_filter_offset ||
	    column_meta_data.bloom_filter_offset <= 0) {
		return false;
	}
	// TODO check length against file length!

	auto &transport = reinterpret_cast<ThriftFileTransport &>(*file_proto.getTransport());
	transport.SetLocation(column_meta_data.bloom_filter_offset);
	if (column_meta_data.__isset.bloom_filter_length && column_meta_data.bloom_filter_length > 0) {
		transport.Prefetch(column_meta_data.bloom_filter_offset, column_meta_data.bloom_filter_length);
	}

	duckdb_parquet::BloomFilterHeader filter_header;
	// TODO the bloom filter could be encrypted, too, so need to double check that this is NOT the case
	filter_header.read(&file_proto);
	if (!filter_header.algorithm.__isset.BLOCK || !filter_header.compression.__isset.UNCOMPRESSED ||
	    !filter_header.hash.__isset.XXHASH) {
		return false;
	}

	auto new_buffer = make_uniq<ResizeableBuffer>(allocator, filter_header.numBytes);
	transport.read(new_buffer->ptr, filter_header.numBytes);
	ParquetBloomFilter bloom_filter(std::move(new_buffer));
	return ApplyBloomFilter(duckdb_filter, bloom_filter);
}

ParquetBloomFilter::ParquetBloomFilter(idx_t num_entries, double bloom_filter_false_positive_ratio) {

	// aim for hit ratio of 0.01%
	// see http://tfk.mit.edu/pdf/bloom.pdf
	double f = bloom_filter_false_positive_ratio;
	double k = 8.0;
	double n = num_entries;
	double m = -k * n / std::log(1 - std::pow(f, 1 / k));
	auto b = MaxValue<idx_t>(NextPowerOfTwo(m / k) / 32, 1);

	D_ASSERT(b > 0 && IsPowerOfTwo(b));

	data = make_uniq<ResizeableBuffer>(Allocator::DefaultAllocator(), sizeof(ParquetBloomBlock) * b);
	data->zero();
	block_count = data->len / sizeof(ParquetBloomBlock);
	D_ASSERT(data->len % sizeof(ParquetBloomBlock) == 0);
}

ParquetBloomFilter::ParquetBloomFilter(unique_ptr<ResizeableBuffer> data_p) {
	D_ASSERT(data_p->len % sizeof(ParquetBloomBlock) == 0);
	data = std::move(data_p);
	block_count = data->len / sizeof(ParquetBloomBlock);
	D_ASSERT(data->len % sizeof(ParquetBloomBlock) == 0);
}

void ParquetBloomFilter::FilterInsert(uint64_t x) {
	auto blocks = (ParquetBloomBlock *)(data->ptr);
	uint64_t i = ((x >> 32) * block_count) >> 32;
	auto &b = blocks[i];
	ParquetBloomBlock::BlockInsert(b, x);
}

bool ParquetBloomFilter::FilterCheck(uint64_t x) {
	auto blocks = (ParquetBloomBlock *)(data->ptr);
	auto i = ((x >> 32) * block_count) >> 32;
	return ParquetBloomBlock::BlockCheck(blocks[i], x);
}

// compiler optimizes this into a single instruction (popcnt)
static uint8_t PopCnt64(uint64_t n) {
	uint8_t c = 0;
	for (; n; ++c) {
		n &= n - 1;
	}
	return c;
}

double ParquetBloomFilter::OneRatio() {
	auto bloom_ptr = (uint64_t *)data->ptr;
	idx_t one_count = 0;
	for (idx_t b_idx = 0; b_idx < data->len / sizeof(uint64_t); ++b_idx) {
		one_count += PopCnt64(bloom_ptr[b_idx]);
	}
	return one_count / (data->len * 8.0);
}

ResizeableBuffer *ParquetBloomFilter::Get() {
	return data.get();
}

} // namespace duckdb
