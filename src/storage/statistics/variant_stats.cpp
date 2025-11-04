#include "duckdb/storage/statistics/variant_stats.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/function/scalar/variant_utils.hpp"

#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/types/variant.hpp"

#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

#include "duckdb/common/types/variant_visitor.hpp"

namespace duckdb {

static void AssertVariant(const BaseStatistics &stats) {
	if (DUCKDB_UNLIKELY(stats.GetStatsType() != StatisticsType::VARIANT_STATS)) {
		throw InternalException(
		    "Calling a VariantStats method on BaseStatistics that are not of type VARIANT, but of type %s",
		    EnumUtil::ToString(stats.GetStatsType()));
	}
}

void VariantColumnStatsData::SetType(VariantLogicalType type) {
	type_counts[static_cast<uint8_t>(type)]++;
	total_count++;
}

VariantColumnStatsData &VariantStatsData::GetOrCreateElement(idx_t parent_index) {
	auto &parent_column = GetColumnStats(parent_index);

	idx_t element_stats = parent_column.element_stats;
	if (parent_column.element_stats == DConstants::INVALID_INDEX) {
		parent_column.element_stats = columns.size();
		element_stats = parent_column.element_stats;
		columns.emplace_back(element_stats);
	}
	return GetColumnStats(element_stats);
}

VariantColumnStatsData &VariantStatsData::GetOrCreateField(idx_t parent_index, const string &name) {
	auto &parent_column = columns[parent_index];
	auto it = parent_column.field_stats.find(name);

	idx_t field_stats;
	if (it == parent_column.field_stats.end()) {
		it = parent_column.field_stats.emplace(name, columns.size()).first;
		field_stats = it->second;
		columns.emplace_back(field_stats);
	} else {
		field_stats = it->second;
	}
	return GetColumnStats(field_stats);
}

void VariantStatsData::SetEmpty() {
	D_ASSERT(columns.empty());
	columns.emplace_back(0);
}

void VariantStatsData::SetUnknown() {
	D_ASSERT(columns.empty());
	columns.emplace_back(0);
}

void VariantStatsData::Merge(const VariantStatsData &other) {
	// throw NotImplementedException("VariantStatsData::Merge");
}

void VariantStatsData::Update(const Value &value) {
	// throw NotImplementedException("VariantStatsData::Update");
}

VariantColumnStatsData &VariantStatsData::GetColumnStats(idx_t index) {
	D_ASSERT(columns.size() > index);
	return columns[index];
}

const VariantColumnStatsData &VariantStatsData::GetColumnStats(idx_t index) const {
	D_ASSERT(columns.size() > index);
	return columns[index];
}

LogicalType VariantStats::GetUnshreddedType() {
	return LogicalType::STRUCT(StructType::GetChildTypes(LogicalType::VARIANT()));
}

static LogicalType ProduceShreddedType(VariantLogicalType type_id) {
	switch (type_id) {
	case VariantLogicalType::BOOL_TRUE:
	case VariantLogicalType::BOOL_FALSE:
		return LogicalTypeId::BOOLEAN;
	case VariantLogicalType::INT8:
		return LogicalTypeId::TINYINT;
	case VariantLogicalType::INT16:
		return LogicalTypeId::SMALLINT;
	case VariantLogicalType::INT32:
		return LogicalTypeId::INTEGER;
	case VariantLogicalType::INT64:
		return LogicalTypeId::BIGINT;
	case VariantLogicalType::INT128:
		return LogicalTypeId::HUGEINT;
	case VariantLogicalType::UINT8:
		return LogicalTypeId::UTINYINT;
	case VariantLogicalType::UINT16:
		return LogicalTypeId::USMALLINT;
	case VariantLogicalType::UINT32:
		return LogicalTypeId::UINTEGER;
	case VariantLogicalType::UINT64:
		return LogicalTypeId::UBIGINT;
	case VariantLogicalType::UINT128:
		return LogicalTypeId::UHUGEINT;
	case VariantLogicalType::FLOAT:
		return LogicalTypeId::FLOAT;
	case VariantLogicalType::DOUBLE:
		return LogicalTypeId::DOUBLE;
	case VariantLogicalType::DECIMAL:
		throw InternalException("Can't shred on DECIMAL");
	case VariantLogicalType::VARCHAR:
		return LogicalTypeId::VARCHAR;
	case VariantLogicalType::BLOB:
		return LogicalTypeId::BLOB;
	case VariantLogicalType::UUID:
		return LogicalTypeId::UUID;
	case VariantLogicalType::DATE:
		return LogicalTypeId::DATE;
	case VariantLogicalType::TIME_MICROS:
		return LogicalTypeId::TIME;
	case VariantLogicalType::TIME_NANOS:
		return LogicalTypeId::TIME_NS;
	case VariantLogicalType::TIMESTAMP_SEC:
		return LogicalTypeId::TIMESTAMP_SEC;
	case VariantLogicalType::TIMESTAMP_MILIS:
		return LogicalTypeId::TIMESTAMP_MS;
	case VariantLogicalType::TIMESTAMP_MICROS:
		return LogicalTypeId::TIMESTAMP;
	case VariantLogicalType::TIMESTAMP_NANOS:
		return LogicalTypeId::TIMESTAMP_NS;
	case VariantLogicalType::TIME_MICROS_TZ:
		return LogicalTypeId::TIME_TZ;
	case VariantLogicalType::TIMESTAMP_MICROS_TZ:
		return LogicalTypeId::TIMESTAMP_TZ;
	case VariantLogicalType::INTERVAL:
		return LogicalTypeId::INTERVAL;
	case VariantLogicalType::BIGNUM:
		return LogicalTypeId::BIGNUM;
	case VariantLogicalType::BITSTRING:
		return LogicalTypeId::BIT;
	case VariantLogicalType::GEOMETRY:
		return LogicalTypeId::GEOMETRY;
	case VariantLogicalType::OBJECT:
	case VariantLogicalType::ARRAY:
		throw InternalException("Already handled above");
	default:
		throw NotImplementedException("Shredding on VariantLogicalType::%s not supported yet",
		                              EnumUtil::ToString(type_id));
	}
}

static LogicalType SetShreddedType(const LogicalType &typed_value) {
	child_list_t<LogicalType> child_types;
	child_types.emplace_back("untyped_value_index", LogicalType::UINTEGER);
	child_types.emplace_back("typed_value", typed_value);
	return LogicalType::STRUCT(child_types);
}

static bool GetShreddedTypeInternal(const VariantStatsData &data, const VariantColumnStatsData &column,
                                    LogicalType &out_type) {
	idx_t max_count = 0;
	uint8_t type_index;
	if (column.type_counts[0] == column.total_count) {
		//! All NULL, emit INT32
		out_type = SetShreddedType(LogicalTypeId::INTEGER);
		return true;
	}

	//! Skip the 'VARIANT_NULL' type, we can't shred on NULL
	for (uint8_t i = 1; i < static_cast<uint8_t>(VariantLogicalType::ENUM_SIZE); i++) {
		if (i == static_cast<uint8_t>(VariantLogicalType::DECIMAL)) {
			//! Can't shred on DECIMAL currently
			continue;
		}
		idx_t count = column.type_counts[i];
		if (!max_count || count > max_count) {
			max_count = count;
			type_index = i;
		}
	}

	if (!max_count) {
		return false;
	}

	if (type_index == static_cast<uint8_t>(VariantLogicalType::OBJECT)) {
		child_list_t<LogicalType> child_types;
		for (auto &entry : column.field_stats) {
			auto &child_column = data.GetColumnStats(entry.second);
			LogicalType child_type;
			if (GetShreddedTypeInternal(data, child_column, child_type)) {
				child_types.emplace_back(entry.first, child_type);
			}
		}
		if (child_types.empty()) {
			return false;
		}
		auto shredded_type = LogicalType::STRUCT(child_types);
		out_type = SetShreddedType(shredded_type);
		return true;
	}
	if (type_index == static_cast<uint8_t>(VariantLogicalType::ARRAY)) {
		D_ASSERT(column.element_stats != DConstants::INVALID_INDEX);
		auto &element_column = data.GetColumnStats(column.element_stats);
		LogicalType element_type;
		if (!GetShreddedTypeInternal(data, element_column, element_type)) {
			return false;
		}
		auto shredded_type = LogicalType::LIST(element_type);
		out_type = SetShreddedType(shredded_type);
		return true;
	}
	auto type_id = static_cast<VariantLogicalType>(type_index);

	auto shredded_type = ProduceShreddedType(type_id);
	out_type = SetShreddedType(shredded_type);
	return true;
}

LogicalType VariantStats::GetShreddedType(const BaseStatistics &stats) {
	auto &data = GetDataUnsafe(stats);
	auto &root_column = data.GetColumnStats(0);

	child_list_t<LogicalType> child_types;
	child_types.emplace_back("unshredded", GetUnshreddedType());
	LogicalType shredded_type;
	if (GetShreddedTypeInternal(data, root_column, shredded_type)) {
		child_types.emplace_back("shredded", shredded_type);
	}
	return LogicalType::STRUCT(child_types);
}

void VariantStats::CreateUnshreddedStats(BaseStatistics &stats) {
	BaseStatistics::Construct(stats.child_stats[0], GetUnshreddedType());
}

void VariantStats::Construct(BaseStatistics &stats) {
	stats.child_stats = unsafe_unique_array<BaseStatistics>(new BaseStatistics[2]);
	CreateUnshreddedStats(stats);
}

BaseStatistics VariantStats::CreateUnknown(LogicalType type) {
	BaseStatistics result(std::move(type));
	result.InitializeUnknown();
	GetDataUnsafe(result).SetUnknown();
	result.child_stats[0].Copy(BaseStatistics::CreateUnknown(GetUnshreddedType()));
	return result;
}

BaseStatistics VariantStats::CreateEmpty(LogicalType type) {
	BaseStatistics result(std::move(type));
	result.InitializeEmpty();
	GetDataUnsafe(result).SetEmpty();
	result.child_stats[0].Copy(BaseStatistics::CreateEmpty(GetUnshreddedType()));
	return result;
}

BaseStatistics VariantStats::CreateShredded(const LogicalType &shredded_type) {
	BaseStatistics result(LogicalType::VARIANT());
	result.InitializeEmpty();
	auto &variant_stats = GetDataUnsafe(result);
	variant_stats.is_shredded = true;
	result.child_stats[0].Copy(BaseStatistics::CreateEmpty(GetUnshreddedType()));
	BaseStatistics::Construct(result.child_stats[1], shredded_type);
	result.child_stats[1].Copy(BaseStatistics::CreateEmpty(shredded_type));
	return result;
}

const BaseStatistics &VariantStats::GetShreddedStats(const BaseStatistics &stats) {
	AssertVariant(stats);
	D_ASSERT(GetDataUnsafe(stats).is_shredded == true);
	return stats.child_stats[1];
}

BaseStatistics &VariantStats::GetShreddedStats(BaseStatistics &stats) {
	AssertVariant(stats);
	D_ASSERT(GetDataUnsafe(stats).is_shredded == true);
	return stats.child_stats[1];
}

const BaseStatistics &VariantStats::GetUnshreddedStats(const BaseStatistics &stats) {
	AssertVariant(stats);
	return stats.child_stats[0];
}

BaseStatistics &VariantStats::GetUnshreddedStats(BaseStatistics &stats) {
	AssertVariant(stats);
	return stats.child_stats[0];
}

void VariantStats::SetUnshreddedStats(BaseStatistics &stats, const BaseStatistics &new_stats) {
	AssertVariant(stats);
	stats.child_stats[0].Copy(new_stats);
}

void VariantStats::SetUnshreddedStats(BaseStatistics &stats, unique_ptr<BaseStatistics> new_stats) {
	AssertVariant(stats);
	if (!new_stats) {
		CreateUnshreddedStats(stats);
	} else {
		SetUnshreddedStats(stats, *new_stats);
	}
}

void VariantStats::SetShreddedStats(BaseStatistics &stats, const BaseStatistics &new_stats) {
	AssertVariant(stats);
	auto &data = GetDataUnsafe(stats);
	if (!data.is_shredded) {
		BaseStatistics::Construct(stats.child_stats[1], new_stats.GetType());
		data.is_shredded = true;
	}
	stats.child_stats[1].Copy(new_stats);
}

void VariantStats::Serialize(const BaseStatistics &stats, Serializer &serializer) {
	auto &unshredded_stats = VariantStats::GetUnshreddedStats(stats);

	serializer.WriteList(200, "child_stats", 1,
	                     [&](Serializer::List &list, idx_t i) { list.WriteElement(unshredded_stats); });
}

void VariantStats::Deserialize(Deserializer &deserializer, BaseStatistics &base) {
	auto &type = base.GetType();
	D_ASSERT(type.InternalType() == PhysicalType::STRUCT);
	D_ASSERT(type.id() == LogicalTypeId::VARIANT);

	auto unshredded_type = GetUnshreddedType();
	deserializer.ReadList(200, "child_stats", [&](Deserializer::List &list, idx_t i) {
		deserializer.Set<const LogicalType &>(unshredded_type);
		auto stat = list.ReadElement<BaseStatistics>();
		base.child_stats[i].Copy(stat);
		deserializer.Unset<LogicalType>();
	});
}

string VariantStats::ToString(const BaseStatistics &stats) {
	const auto &data = GetDataUnsafe(stats);
	string result;

	throw NotImplementedException("VariantStats::ToString");
	return result;
}

namespace {

struct VariantStatsVisitor {
	using result_type = void;

	static void VisitNull(VariantStatsData &stats, idx_t stats_column_index) {
		return;
	}
	static void VisitBoolean(bool val, VariantStatsData &stats, idx_t stats_column_index) {
		return;
	}

	static void VisitMetadata(VariantLogicalType type_id, VariantStatsData &stats, idx_t stats_column_index) {
		auto &column_stats = stats.GetColumnStats(stats_column_index);
		column_stats.SetType(type_id);
	}

	template <typename T>
	static void VisitInteger(T val, VariantStatsData &stats, idx_t stats_column_index) {
	}
	static void VisitFloat(float val, VariantStatsData &stats, idx_t stats_column_index) {
	}
	static void VisitDouble(double val, VariantStatsData &stats, idx_t stats_column_index) {
	}
	static void VisitUUID(hugeint_t val, VariantStatsData &stats, idx_t stats_column_index) {
	}
	static void VisitDate(date_t val, VariantStatsData &stats, idx_t stats_column_index) {
	}
	static void VisitInterval(interval_t val, VariantStatsData &stats, idx_t stats_column_index) {
	}
	static void VisitTime(dtime_t val, VariantStatsData &stats, idx_t stats_column_index) {
	}
	static void VisitTimeNanos(dtime_ns_t val, VariantStatsData &stats, idx_t stats_column_index) {
	}
	static void VisitTimeTZ(dtime_tz_t val, VariantStatsData &stats, idx_t stats_column_index) {
	}
	static void VisitTimestampSec(timestamp_sec_t val, VariantStatsData &stats, idx_t stats_column_index) {
	}
	static void VisitTimestampMs(timestamp_ms_t val, VariantStatsData &stats, idx_t stats_column_index) {
	}
	static void VisitTimestamp(timestamp_t val, VariantStatsData &stats, idx_t stats_column_index) {
	}
	static void VisitTimestampNanos(timestamp_ns_t val, VariantStatsData &stats, idx_t stats_column_index) {
	}
	static void VisitTimestampTZ(timestamp_tz_t val, VariantStatsData &stats, idx_t stats_column_index) {
	}
	static void WriteStringInternal(const string_t &str, VariantStatsData &stats, idx_t stats_column_index) {
	}
	static void VisitString(const string_t &str, VariantStatsData &stats, idx_t stats_column_index) {
	}
	static void VisitBlob(const string_t &blob, VariantStatsData &stats, idx_t stats_column_index) {
	}
	static void VisitBignum(const string_t &bignum, VariantStatsData &stats, idx_t stats_column_index) {
	}
	static void VisitGeometry(const string_t &geom, VariantStatsData &stats, idx_t stats_column_index) {
	}
	static void VisitBitstring(const string_t &bits, VariantStatsData &stats, idx_t stats_column_index) {
	}

	template <typename T>
	static void VisitDecimal(T val, uint32_t width, uint32_t scale, VariantStatsData &stats, idx_t stats_column_index) {
		//! FIXME: need to visit to be able to shred on DECIMAL values
	}

	static void VisitArray(const UnifiedVariantVectorData &variant, idx_t row, const VariantNestedData &nested_data,
	                       VariantStatsData &stats, idx_t stats_column_index) {
		auto &element_stats = stats.GetOrCreateElement(stats_column_index);
		auto index = element_stats.index;
		VariantVisitor<VariantStatsVisitor>::VisitArrayItems(variant, row, nested_data, stats, index);
	}

	static void VisitObject(const UnifiedVariantVectorData &variant, idx_t row, const VariantNestedData &nested_data,
	                        VariantStatsData &stats, idx_t stats_column_index) {
		//! Then visit the fields in sorted order
		for (idx_t i = 0; i < nested_data.child_count; i++) {
			auto source_children_idx = nested_data.children_idx + i;

			//! Add the key of the field to the result
			auto keys_index = variant.GetKeysIndex(row, source_children_idx);
			auto &key = variant.GetKey(row, keys_index);

			auto &child_stats = stats.GetOrCreateField(stats_column_index, key.GetString());
			auto index = child_stats.index;

			//! Visit the child value
			auto values_index = variant.GetValuesIndex(row, source_children_idx);
			VariantVisitor<VariantStatsVisitor>::Visit(variant, row, values_index, stats, index);
		}
	}

	static void VisitDefault(VariantLogicalType type_id, const_data_ptr_t, VariantStatsData &stats,
	                         idx_t stats_column_index) {
		throw InternalException("VariantLogicalType(%s) not handled", EnumUtil::ToString(type_id));
	}
};

} // namespace

void VariantStats::Update(BaseStatistics &stats, Vector &vector, idx_t count) {
	auto &data = GetDataUnsafe(stats);

	RecursiveUnifiedVectorFormat recursive_format;
	Vector::RecursiveToUnifiedFormat(vector, count, recursive_format);
	UnifiedVariantVectorData variant(recursive_format);

	for (idx_t i = 0; i < count; i++) {
		VariantVisitor<VariantStatsVisitor>::Visit(variant, i, 0, data, static_cast<idx_t>(0));
	}
}

void VariantStats::Merge(BaseStatistics &stats, const BaseStatistics &other) {
	if (other.GetType().id() == LogicalTypeId::VALIDITY) {
		return;
	}
	//! Merge the unshredded stats
	stats.child_stats[0].Merge(other.child_stats[0]);
}

void VariantStats::Copy(BaseStatistics &stats, const BaseStatistics &other) {
	stats.child_stats[0].Copy(other.child_stats[0]);
	auto &data = VariantStats::GetDataUnsafe(stats);
	auto &other_data = VariantStats::GetDataUnsafe(other);
	if (other_data.is_shredded) {
		if (!data.is_shredded) {
			BaseStatistics::Construct(stats.child_stats[1], other.child_stats[1].GetType());
			data.is_shredded = true;
		}
		//! FIXME: assumes equal shredding type?
		stats.child_stats[1].Copy(other.child_stats[1]);
	}
}

void VariantStats::Verify(const BaseStatistics &stats, Vector &vector, const SelectionVector &sel, idx_t count) {
	// TODO: Verify stats
}

const VariantStatsData &VariantStats::GetDataUnsafe(const BaseStatistics &stats) {
	AssertVariant(stats);
	return stats.variant_data;
}

VariantStatsData &VariantStats::GetDataUnsafe(BaseStatistics &stats) {
	AssertVariant(stats);
	return stats.variant_data;
}

} // namespace duckdb
