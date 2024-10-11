#include "parquet_statistics.hpp"

#include "duckdb.hpp"
#include "parquet_decimal_utils.hpp"
#include "parquet_timestamp.hpp"
#include "string_column_reader.hpp"
#include "struct_column_reader.hpp"
#include "zstd/common/xxhash.h"

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
	case LogicalType::VARCHAR:
	case LogicalType::BLOB:
		if (Value::StringIsValid(stats)) {
			return Value(stats);
		} else {
			return Value(Blob::ToString(string_t(stats)));
		}
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
			return Value::TIMESTAMPTZ(timestamp_value);
		} else {
			return Value::TIMESTAMP(timestamp_value);
		}
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

// bloom filter stuff
// see https://github.com/apache/parquet-format/blob/master/BloomFilter.md

static uint32_t parquet_bloom_salt[8] = {0x47b6137bU, 0x44974d91U, 0x8824ad5bU, 0xa2b7289dU,
                                         0x705495c7U, 0x2df1424bU, 0x9efc4947U, 0x5c6bfb31U};

struct ParquetBloomMaskResult {
	uint8_t bit_set[8] = {0};
};

struct ParquetBloomBlock {
	uint32_t block[8] = {0};

	static bool check_bit(uint32_t &x, const uint8_t i) {
		D_ASSERT(i < 32);
		return (x >> i) & (uint32_t)1;
	}

	static void set_bit(uint32_t &x, const uint8_t i) {
		D_ASSERT(i < 32);
		x |= (uint32_t)1 << i;
		D_ASSERT(check_bit(x, i));
	}

	/*
	    static ParquetBloomBlock Mask(uint32_t x) {
	        ParquetBloomBlock result;
	        for (idx_t i = 0; i < 8; i++) {
	            auto y = x * parquet_bloom_salt[i];
	            set_bit(result.block[i], y >> 27);
	        }
	        return result;
	    }*/

	static ParquetBloomMaskResult Mask(uint32_t x) {
		ParquetBloomMaskResult result;
		for (idx_t i = 0; i < 8; i++) {
			result.bit_set[i] = (x * parquet_bloom_salt[i]) >> 27;
		}
		return result;
	}

	/*
	static void BlockInsert(ParquetBloomBlock& b, uint32_t x) {
	    auto masked = Mask(x);
	    for (idx_t i = 0; i < 8; i++) {
	        for (idx_t j = 0; j < 32; j++) {
	            if (check_bit(masked.block[i], j)) {
	                b.set_bit(b.block[i], j);
	                D_ASSERT(check_bit(b.block[i], j));
	            }
	        }
	    }
	}
*/

	//  Similarly, block_check returns true when every bit that is set in the result of mask is also set in the block.
	/*
	static bool BlockCheck(ParquetBloomBlock& b, uint32_t x) {
	    ParquetBloomBlock masked = Mask(x);
	    for (idx_t i = 0; i < 8; i++) {
	        for (idx_t j = 0; j < 32; j++) {
	            // TODO this could be simplified by changing the format of mask into key/value pairs
	            if (check_bit(masked.block[i], j)) {
	                if (!check_bit(b.block[i], j)) {
	                    return false;
	                }
	            }
	        }
	    }
	    return true;
	}
	*/

	static bool BlockCheck(ParquetBloomBlock &b, uint32_t x) {
		auto masked = Mask(x);
		for (idx_t i = 0; i < 8; i++) {
			if (!check_bit(b.block[i], masked.bit_set[i])) {
				return false;
			}
		}
		return true;
	}
};

struct ParquetBloomFilter {
	/*
	void FilterInsert(uint64_t x) {
	    auto blocks = (ParquetBloomBlock*)(data->ptr);
	    auto block_count = data->len/sizeof(ParquetBloomBlock);
	    uint64_t i = ((x >> 32) * block_count) >> 32;
	    auto b = blocks[i];
	    ParquetBloomBlock::BlockInsert(b, x);
	}
*/
	bool FilterCheck(uint64_t x) {
		auto blocks = (ParquetBloomBlock *)(data->ptr);
		// TODO this can be cached!
		auto block_count = data->len / sizeof(ParquetBloomBlock);
		D_ASSERT(data->len % sizeof(ParquetBloomBlock) == 0);
		auto i = ((x >> 32) * block_count) >> 32;
		return ParquetBloomBlock::BlockCheck(blocks[i], x);
	}

	unique_ptr<ResizeableBuffer> data;
};

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
	case PhysicalType::BOOL:
	case PhysicalType::UINT8:
	case PhysicalType::INT8:
		return ValueXH64FixedWidth<uint8_t>(constant);
	case PhysicalType::UINT16:
	case PhysicalType::INT16:
		return ValueXH64FixedWidth<uint16_t>(constant);
	case PhysicalType::UINT32:
	case PhysicalType::INT32:
		return ValueXH64FixedWidth<uint32_t>(constant);
	case PhysicalType::UINT64:
	case PhysicalType::INT64:
		return ValueXH64FixedWidth<uint64_t>(constant);
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

bool ParquetStatisticsUtils::BloomFilterExcludes(const TableFilter &duckdb_filter,
                                                 const duckdb_parquet::ColumnMetaData &column_meta_data,
                                                 TProtocol &file_proto, Allocator &allocator) {
	if (!HasFilterConstants(duckdb_filter) || !column_meta_data.__isset.bloom_filter_offset ||
	    column_meta_data.bloom_filter_offset <= 0) {
		return false;
	}

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

	ParquetBloomFilter bloom_filter;
	bloom_filter.data = make_uniq<ResizeableBuffer>(allocator, filter_header.numBytes);
	transport.read(bloom_filter.data->ptr, filter_header.numBytes);

	return ApplyBloomFilter(duckdb_filter, bloom_filter);
}

} // namespace duckdb
