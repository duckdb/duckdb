#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/common/vector/variant_vector.hpp"
#include "duckdb/common/types/variant_value.hpp"
#include "duckdb/common/types/variant_iterator.hpp"
#include "yyjson.hpp"

#include "duckdb/common/serializer/varint.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/datetime.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/variant.hpp"
#include "duckdb/common/hugeint.hpp"
#include "duckdb/function/scalar/variant_utils.hpp"
#include "duckdb/common/string_map_set.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/owning_string_map.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/limits.hpp"

using namespace duckdb_yyjson; // NOLINT

namespace duckdb {

void VariantValue::AddChild(const string &key, VariantValue &&val) {
	D_ASSERT(value_type == VariantValueType::OBJECT);
	if (val.IsMissing()) {
		return;
	}
	object_children.emplace(key, std::move(val));
}

void VariantValue::AddItem(VariantValue &&val) {
	D_ASSERT(value_type == VariantValueType::ARRAY);
	if (val.IsMissing()) {
		//! SPEC: If a Variant is missing in a context where a value is required, readers must return a Variant null
		val = VariantValue::NullValue();
	}
	array_items.push_back(std::move(val));
}

void VariantValue::SetItems(vector<VariantValue> &&values) {
	D_ASSERT(value_type == VariantValueType::ARRAY);
	for (auto &value : values) {
		if (value.IsMissing()) {
			//! SPEC: If a Variant is missing in a context where a value is required, readers must return a Variant null
			value = VariantValue::NullValue();
		}
	}
	array_items = std::move(values);
}

void VariantValue::ReserveItems(idx_t count) {
	array_items.reserve(count);
}

void VariantValue::AddItems(vector<VariantValue>::iterator begin, vector<VariantValue>::iterator end) {
	D_ASSERT(value_type == VariantValueType::ARRAY);
	for (; begin != end; begin++) {
		auto &value = *begin;
		if (value.IsMissing()) {
			//! SPEC: If a Variant is missing in a context where a value is required, readers must return a Variant null
			value = VariantValue::NullValue();
		}
		array_items.push_back(std::move(value));
	}
}

map<string, VariantValue> VariantValue::TakeObjectChildren() {
	D_ASSERT(value_type == VariantValueType::OBJECT);
	return std::move(object_children);
}

const map<string, VariantValue> &VariantValue::ObjectChildren() const {
	return object_children;
}

const vector<VariantValue> &VariantValue::ArrayItems() const {
	return array_items;
}

Value VariantValue::GetValue(const Value &variant_val) {
	D_ASSERT(variant_val.type().id() == LogicalTypeId::VARIANT && !variant_val.IsNull());
	Vector tmp(variant_val, count_t(1));
	RecursiveUnifiedVectorFormat format;
	Vector::RecursiveToUnifiedFormat(tmp, format);
	UnifiedVariantVectorData vector_data(format);
	return VariantUtils::ConvertVariantToValue(vector_data, 0, 0);
}

//===--------------------------------------------------------------------===//
// Building a VARIANT in a single pass
//===--------------------------------------------------------------------===//
// The canonical (unshredded) VARIANT layout is built directly while traversing the source tree once.
// Rather than a separate "analyze sizes" pass followed by an in-place "convert" pass, the blob bytes
// and the (values / children / keys) entries are accumulated into growable buffers, and copied into
// the result vectors once the per-row sizes are known. The data is sourced either from a
// vector<VariantValue> (the parquet reader) or from a VariantIterator (unshredding).

namespace {

//! Sentinel marking an array child (whose 'key_id' is NULL)
constexpr uint32_t INVALID_KEY = NumericLimits<uint32_t>::Maximum();

//! Accumulates the canonical representation of a single chunk while traversing the source once.
struct VariantBuilder {
	explicit VariantBuilder(OrderedOwningStringMap<uint32_t> &dictionary) : dictionary(dictionary) {
	}

	//! values: (type_id, byte_offset) in pre-order
	vector<uint8_t> type_ids;
	vector<uint32_t> byte_offsets;
	//! children: (key_id, value_id) - key_id is INVALID_KEY for array elements
	vector<uint32_t> child_key_ids;
	vector<uint32_t> child_value_ids;
	//! one (unsorted) dictionary index per object-child key slot
	vector<uint32_t> key_slots;
	//! the blob of the row currently being built (reused across rows)
	string blob;
	//! maps a key string to its (unsorted) dictionary index, owned by the result's keys vector
	OrderedOwningStringMap<uint32_t> &dictionary;

	//! the offsets at which the current row's entries begin
	idx_t row_values = 0;
	idx_t row_children = 0;
	idx_t row_keys = 0;

	void BeginRow() {
		row_values = type_ids.size();
		row_children = child_value_ids.size();
		row_keys = key_slots.size();
		blob.clear();
	}
	//! The current value / child / key index, relative to the start of the row
	uint32_t LocalValue() const {
		return NumericCast<uint32_t>(type_ids.size() - row_values);
	}
	uint32_t LocalChild() const {
		return NumericCast<uint32_t>(child_value_ids.size() - row_children);
	}
	uint32_t LocalKey() const {
		return NumericCast<uint32_t>(key_slots.size() - row_keys);
	}
};

void AppendVarint(string &blob, uint32_t value) {
	auto size = GetVarintSize(value);
	auto pos = blob.size();
	blob.resize(pos + size);
	VarintEncode<uint32_t>(value, data_ptr_cast(blob.data()) + pos);
}

template <class T>
void AppendFixed(string &blob, T value) {
	auto pos = blob.size();
	blob.resize(pos + sizeof(T));
	Store<T>(value, data_ptr_cast(blob.data()) + pos);
}

void AppendBytes(string &blob, const_data_ptr_t data, idx_t size) {
	blob.append(const_char_ptr_cast(data), size);
}

uint32_t GetOrCreateIndex(OrderedOwningStringMap<uint32_t> &dictionary, const string_t &key) {
	auto unsorted_idx = dictionary.size();
	//! This will later be remapped to the sorted idx (see FinalizeVariantKeys in 'to_variant.cpp')
	return dictionary.emplace(std::make_pair(key, unsorted_idx)).first->second;
}

//! The (fixed) blob payload size of a fixed-width primitive VariantLogicalType
idx_t VariantFixedPayloadSize(VariantLogicalType type_id) {
	switch (type_id) {
	case VariantLogicalType::VARIANT_NULL:
	case VariantLogicalType::BOOL_TRUE:
	case VariantLogicalType::BOOL_FALSE:
		return 0;
	case VariantLogicalType::INT8:
		return sizeof(int8_t);
	case VariantLogicalType::INT16:
		return sizeof(int16_t);
	case VariantLogicalType::INT32:
		return sizeof(int32_t);
	case VariantLogicalType::INT64:
		return sizeof(int64_t);
	case VariantLogicalType::INT128:
		return sizeof(hugeint_t);
	case VariantLogicalType::UINT8:
		return sizeof(uint8_t);
	case VariantLogicalType::UINT16:
		return sizeof(uint16_t);
	case VariantLogicalType::UINT32:
		return sizeof(uint32_t);
	case VariantLogicalType::UINT64:
		return sizeof(uint64_t);
	case VariantLogicalType::UINT128:
		return sizeof(uhugeint_t);
	case VariantLogicalType::FLOAT:
		return sizeof(float);
	case VariantLogicalType::DOUBLE:
		return sizeof(double);
	case VariantLogicalType::UUID:
		return sizeof(hugeint_t);
	case VariantLogicalType::DATE:
		return sizeof(date_t);
	case VariantLogicalType::TIME_MICROS:
		return sizeof(dtime_t);
	case VariantLogicalType::TIME_NANOS:
		return sizeof(dtime_ns_t);
	case VariantLogicalType::TIME_MICROS_TZ:
		return sizeof(dtime_tz_t);
	case VariantLogicalType::TIMESTAMP_SEC:
		return sizeof(timestamp_sec_t);
	case VariantLogicalType::TIMESTAMP_MILIS:
		return sizeof(timestamp_ms_t);
	case VariantLogicalType::TIMESTAMP_MICROS:
		return sizeof(timestamp_t);
	case VariantLogicalType::TIMESTAMP_NANOS:
		return sizeof(timestamp_ns_t);
	case VariantLogicalType::TIMESTAMP_MICROS_TZ:
		return sizeof(timestamp_tz_t);
	case VariantLogicalType::TIMESTAMP_NANOS_TZ:
		return sizeof(timestamp_tz_ns_t);
	case VariantLogicalType::INTERVAL:
		return sizeof(interval_t);
	default:
		throw InternalException("Unexpected VariantLogicalType (%d) when building a VARIANT",
		                        static_cast<int>(type_id));
	}
}

//! Whether 'type_id' is encoded as a (length-prefixed) string in the blob
bool VariantIsStringType(VariantLogicalType type_id) {
	switch (type_id) {
	case VariantLogicalType::VARCHAR:
	case VariantLogicalType::BLOB:
	case VariantLogicalType::BIGNUM:
	case VariantLogicalType::BITSTRING:
	case VariantLogicalType::GEOMETRY:
		return true;
	default:
		return false;
	}
}

//! Collect the (non-missing) object children in lexicographic key order
vector<VariantObjectEntry> CollectObjectChildren(const VariantNode &it) {
	vector<VariantObjectEntry> children;
	for (auto &entry : it.GetObjectChildren(VariantIterationOrder::LEXICOGRAPHIC)) {
		children.push_back(entry);
	}
	return children;
}

//===--------------------------------------------------------------------===//
// Emit (source: VariantNode)
//===--------------------------------------------------------------------===//
void EmitIterator(const VariantNode &it, VariantBuilder &builder) {
	auto byte_offset = NumericCast<uint32_t>(builder.blob.size());
	if (it.IsNull() || it.IsMissing()) {
		builder.type_ids.push_back(static_cast<uint8_t>(VariantLogicalType::VARIANT_NULL));
		builder.byte_offsets.push_back(byte_offset);
		return;
	}

	auto type_id = it.GetTypeId();
	switch (type_id) {
	case VariantLogicalType::OBJECT: {
		auto children = CollectObjectChildren(it);
		builder.type_ids.push_back(static_cast<uint8_t>(VariantLogicalType::OBJECT));
		builder.byte_offsets.push_back(byte_offset);
		AppendVarint(builder.blob, NumericCast<uint32_t>(children.size()));
		if (!children.empty()) {
			AppendVarint(builder.blob, builder.LocalChild());
			auto block = builder.child_value_ids.size();
			builder.child_value_ids.resize(block + children.size());
			builder.child_key_ids.resize(block + children.size());
			for (idx_t i = 0; i < children.size(); i++) {
				builder.child_value_ids[block + i] = builder.LocalValue();
				builder.child_key_ids[block + i] = builder.LocalKey();
				builder.key_slots.push_back(GetOrCreateIndex(builder.dictionary, children[i].key));
				EmitIterator(children[i].value, builder);
			}
		}
		break;
	}
	case VariantLogicalType::ARRAY: {
		auto array = it.GetArrayChildren();
		auto length = array.size();
		builder.type_ids.push_back(static_cast<uint8_t>(VariantLogicalType::ARRAY));
		builder.byte_offsets.push_back(byte_offset);
		AppendVarint(builder.blob, NumericCast<uint32_t>(length));
		if (length) {
			AppendVarint(builder.blob, builder.LocalChild());
			auto block = builder.child_value_ids.size();
			builder.child_value_ids.resize(block + length);
			builder.child_key_ids.resize(block + length);
			for (idx_t i = 0; i < length; i++) {
				builder.child_value_ids[block + i] = builder.LocalValue();
				builder.child_key_ids[block + i] = INVALID_KEY;
				EmitIterator(array[i], builder);
			}
		}
		break;
	}
	case VariantLogicalType::DECIMAL: {
		auto decimal = it.GetDecimal();
		builder.type_ids.push_back(static_cast<uint8_t>(VariantLogicalType::DECIMAL));
		builder.byte_offsets.push_back(byte_offset);
		AppendVarint(builder.blob, decimal.width);
		AppendVarint(builder.blob, decimal.scale);
		AppendBytes(builder.blob, decimal.value_ptr, GetTypeIdSize(decimal.GetPhysicalType()));
		break;
	}
	default:
		builder.type_ids.push_back(static_cast<uint8_t>(type_id));
		builder.byte_offsets.push_back(byte_offset);
		if (VariantIsStringType(type_id)) {
			auto str = it.GetString();
			AppendVarint(builder.blob, NumericCast<uint32_t>(str.GetSize()));
			AppendBytes(builder.blob, const_data_ptr_cast(str.GetData()), str.GetSize());
		} else {
			auto size = VariantFixedPayloadSize(type_id);
			if (size) {
				AppendBytes(builder.blob, it.GetDataPointer(), size);
			}
		}
		break;
	}
}

//===--------------------------------------------------------------------===//
// Emit (source: VariantValue)
//===--------------------------------------------------------------------===//
void EmitPrimitiveValue(const Value &primitive, VariantBuilder &builder, uint32_t byte_offset) {
	auto &blob = builder.blob;
	auto type_id = primitive.type().id();
	VariantLogicalType variant_type;
	switch (type_id) {
	case LogicalTypeId::BOOLEAN:
		variant_type = primitive.GetValue<bool>() ? VariantLogicalType::BOOL_TRUE : VariantLogicalType::BOOL_FALSE;
		break;
	case LogicalTypeId::SQLNULL:
		variant_type = VariantLogicalType::VARIANT_NULL;
		break;
	case LogicalTypeId::TINYINT:
		variant_type = VariantLogicalType::INT8;
		AppendFixed(blob, primitive.GetValueUnsafe<int8_t>());
		break;
	case LogicalTypeId::SMALLINT:
		variant_type = VariantLogicalType::INT16;
		AppendFixed(blob, primitive.GetValueUnsafe<int16_t>());
		break;
	case LogicalTypeId::INTEGER:
		variant_type = VariantLogicalType::INT32;
		AppendFixed(blob, primitive.GetValueUnsafe<int32_t>());
		break;
	case LogicalTypeId::BIGINT:
		variant_type = VariantLogicalType::INT64;
		AppendFixed(blob, primitive.GetValueUnsafe<int64_t>());
		break;
	case LogicalTypeId::HUGEINT:
		variant_type = VariantLogicalType::INT128;
		AppendFixed(blob, primitive.GetValueUnsafe<hugeint_t>());
		break;
	case LogicalTypeId::UTINYINT:
		variant_type = VariantLogicalType::UINT8;
		AppendFixed(blob, primitive.GetValueUnsafe<uint8_t>());
		break;
	case LogicalTypeId::USMALLINT:
		variant_type = VariantLogicalType::UINT16;
		AppendFixed(blob, primitive.GetValueUnsafe<uint16_t>());
		break;
	case LogicalTypeId::UINTEGER:
		variant_type = VariantLogicalType::UINT32;
		AppendFixed(blob, primitive.GetValueUnsafe<uint32_t>());
		break;
	case LogicalTypeId::UBIGINT:
		variant_type = VariantLogicalType::UINT64;
		AppendFixed(blob, primitive.GetValueUnsafe<uint64_t>());
		break;
	case LogicalTypeId::UHUGEINT:
		variant_type = VariantLogicalType::UINT128;
		AppendFixed(blob, primitive.GetValueUnsafe<uhugeint_t>());
		break;
	case LogicalTypeId::DOUBLE:
		variant_type = VariantLogicalType::DOUBLE;
		AppendFixed(blob, primitive.GetValueUnsafe<double>());
		break;
	case LogicalTypeId::FLOAT:
		variant_type = VariantLogicalType::FLOAT;
		AppendFixed(blob, primitive.GetValueUnsafe<float>());
		break;
	case LogicalTypeId::DATE:
		variant_type = VariantLogicalType::DATE;
		AppendFixed(blob, primitive.GetValueUnsafe<date_t>());
		break;
	case LogicalTypeId::TIMESTAMP_TZ:
		variant_type = VariantLogicalType::TIMESTAMP_MICROS_TZ;
		AppendFixed(blob, primitive.GetValueUnsafe<timestamp_tz_t>());
		break;
	case LogicalTypeId::TIMESTAMP_TZ_NS:
		variant_type = VariantLogicalType::TIMESTAMP_NANOS_TZ;
		AppendFixed(blob, primitive.GetValueUnsafe<timestamp_tz_ns_t>());
		break;
	case LogicalTypeId::TIMESTAMP:
		variant_type = VariantLogicalType::TIMESTAMP_MICROS;
		AppendFixed(blob, primitive.GetValueUnsafe<timestamp_t>());
		break;
	case LogicalTypeId::TIMESTAMP_SEC:
		variant_type = VariantLogicalType::TIMESTAMP_SEC;
		AppendFixed(blob, primitive.GetValueUnsafe<timestamp_sec_t>());
		break;
	case LogicalTypeId::TIMESTAMP_MS:
		variant_type = VariantLogicalType::TIMESTAMP_MILIS;
		AppendFixed(blob, primitive.GetValueUnsafe<timestamp_ms_t>());
		break;
	case LogicalTypeId::TIME:
		variant_type = VariantLogicalType::TIME_MICROS;
		AppendFixed(blob, primitive.GetValueUnsafe<dtime_t>());
		break;
	case LogicalTypeId::TIME_NS:
		variant_type = VariantLogicalType::TIME_NANOS;
		AppendFixed(blob, primitive.GetValueUnsafe<dtime_ns_t>());
		break;
	case LogicalTypeId::TIME_TZ:
		variant_type = VariantLogicalType::TIME_MICROS_TZ;
		AppendFixed(blob, primitive.GetValueUnsafe<dtime_tz_t>());
		break;
	case LogicalTypeId::TIMESTAMP_NS:
		variant_type = VariantLogicalType::TIMESTAMP_NANOS;
		AppendFixed(blob, primitive.GetValueUnsafe<timestamp_ns_t>());
		break;
	case LogicalTypeId::INTERVAL:
		variant_type = VariantLogicalType::INTERVAL;
		AppendFixed(blob, primitive.GetValueUnsafe<interval_t>());
		break;
	case LogicalTypeId::UUID:
		variant_type = VariantLogicalType::UUID;
		AppendFixed(blob, primitive.GetValueUnsafe<hugeint_t>());
		break;
	case LogicalTypeId::DECIMAL: {
		variant_type = VariantLogicalType::DECIMAL;
		auto &type = primitive.type();
		uint8_t width;
		uint8_t scale;
		type.GetDecimalProperties(width, scale);
		AppendVarint(blob, width);
		AppendVarint(blob, scale);
		switch (type.InternalType()) {
		case PhysicalType::INT16:
			AppendFixed(blob, primitive.GetValueUnsafe<int16_t>());
			break;
		case PhysicalType::INT32:
			AppendFixed(blob, primitive.GetValueUnsafe<int32_t>());
			break;
		case PhysicalType::INT64:
			AppendFixed(blob, primitive.GetValueUnsafe<int64_t>());
			break;
		case PhysicalType::INT128:
			AppendFixed(blob, primitive.GetValueUnsafe<hugeint_t>());
			break;
		default:
			throw InternalException("Unexpected physical type for Decimal value: %s",
			                        EnumUtil::ToString(type.InternalType()));
		}
		break;
	}
	case LogicalTypeId::BLOB:
	case LogicalTypeId::BIGNUM:
	case LogicalTypeId::BIT:
	case LogicalTypeId::GEOMETRY:
	case LogicalTypeId::VARCHAR: {
		if (type_id == LogicalTypeId::BLOB) {
			variant_type = VariantLogicalType::BLOB;
		} else if (type_id == LogicalTypeId::BIGNUM) {
			variant_type = VariantLogicalType::BIGNUM;
		} else if (type_id == LogicalTypeId::BIT) {
			variant_type = VariantLogicalType::BITSTRING;
		} else if (type_id == LogicalTypeId::GEOMETRY) {
			variant_type = VariantLogicalType::GEOMETRY;
		} else {
			variant_type = VariantLogicalType::VARCHAR;
		}
		auto string_data = primitive.GetValueUnsafe<string_t>();
		AppendVarint(blob, NumericCast<uint32_t>(string_data.GetSize()));
		AppendBytes(blob, const_data_ptr_cast(string_data.GetData()), string_data.GetSize());
		break;
	}
	default:
		throw InternalException("Encountered unrecognized LogicalType in EmitPrimitiveValue: %s",
		                        primitive.type().ToString());
	}
	builder.type_ids.push_back(static_cast<uint8_t>(variant_type));
	builder.byte_offsets.push_back(byte_offset);
}

void EmitValue(const VariantValue &value, VariantBuilder &builder) {
	auto byte_offset = NumericCast<uint32_t>(builder.blob.size());
	switch (value.value_type) {
	case VariantValueType::OBJECT: {
		auto &children = value.ObjectChildren();
		builder.type_ids.push_back(static_cast<uint8_t>(VariantLogicalType::OBJECT));
		builder.byte_offsets.push_back(byte_offset);
		AppendVarint(builder.blob, NumericCast<uint32_t>(children.size()));
		if (!children.empty()) {
			AppendVarint(builder.blob, builder.LocalChild());
			auto block = builder.child_value_ids.size();
			builder.child_value_ids.resize(block + children.size());
			builder.child_key_ids.resize(block + children.size());
			idx_t i = 0;
			for (auto &child : children) {
				builder.child_value_ids[block + i] = builder.LocalValue();
				builder.child_key_ids[block + i] = builder.LocalKey();
				builder.key_slots.push_back(GetOrCreateIndex(builder.dictionary, child.first));
				EmitValue(child.second, builder);
				i++;
			}
		}
		break;
	}
	case VariantValueType::ARRAY: {
		auto &children = value.ArrayItems();
		builder.type_ids.push_back(static_cast<uint8_t>(VariantLogicalType::ARRAY));
		builder.byte_offsets.push_back(byte_offset);
		AppendVarint(builder.blob, NumericCast<uint32_t>(children.size()));
		if (!children.empty()) {
			AppendVarint(builder.blob, builder.LocalChild());
			auto block = builder.child_value_ids.size();
			builder.child_value_ids.resize(block + children.size());
			builder.child_key_ids.resize(block + children.size());
			for (idx_t i = 0; i < children.size(); i++) {
				builder.child_value_ids[block + i] = builder.LocalValue();
				builder.child_key_ids[block + i] = INVALID_KEY;
				EmitValue(children[i], builder);
			}
		}
		break;
	}
	case VariantValueType::PRIMITIVE:
		EmitPrimitiveValue(value.primitive_value, builder, byte_offset);
		break;
	default:
		throw InternalException("VariantValueType not handled");
	}
}

//===--------------------------------------------------------------------===//
// Build driver
//===--------------------------------------------------------------------===//
struct VariantValueSource {
	explicit VariantValueSource(vector<VariantValue> &input) : input(input) {
	}
	//! Emit row 'row' into the builder, returning whether the row is a (SQL) NULL
	bool Emit(idx_t row, VariantBuilder &builder) const {
		auto &value = input[row];
		if (value.IsNull() || value.IsMissing()) {
			return true;
		}
		EmitValue(value, builder);
		return false;
	}

	vector<VariantValue> &input;
};

struct VariantIteratorSource {
	explicit VariantIteratorSource(const VariantIterator &state) : state(state) {
	}
	bool Emit(idx_t row, VariantBuilder &builder) const {
		auto root = state.Root(row);
		//! Root() resolves a missing/absent root to a SQL NULL
		if (root.IsNull()) {
			return true;
		}
		EmitIterator(root, builder);
		return false;
	}

	const VariantIterator &state;
};

template <class SOURCE>
void BuildVariant(const SOURCE &source, idx_t count, Vector &result) {
	if (count == 0) {
		return;
	}

	auto &keys = VariantVector::GetKeys(result);
	auto &keys_entry = ListVector::GetChildMutable(keys);
	auto &children = VariantVector::GetChildren(result);
	auto &values = VariantVector::GetValues(result);
	auto &blob_vector = VariantVector::GetData(result);
	auto blob_data = FlatVector::GetDataMutable<string_t>(blob_vector);

	//! The dictionary is backed by the keys vector's string allocator so the finalized keys are owned
	//! by the result (see FinalizeVariantKeys).
	OrderedOwningStringMap<uint32_t> dictionary(StringVector::GetStringAllocator(keys_entry));
	VariantBuilder builder(dictionary);

	vector<list_entry_t> keys_entries(count);
	vector<list_entry_t> children_entries(count);
	vector<list_entry_t> values_entries(count);

	for (idx_t row = 0; row < count; row++) {
		builder.BeginRow();
		bool is_null = source.Emit(row, builder);
		blob_data[row] = StringVector::AddStringOrBlob(
		    blob_vector, string_t(builder.blob.data(), NumericCast<uint32_t>(builder.blob.size())));
		if (is_null) {
			//! SPEC: If a Variant is missing in a context where a value is required, readers must return a Variant null
			FlatVector::SetNull(result, row, true);
		}
		keys_entries[row] = list_entry_t(builder.row_keys, builder.LocalKey());
		children_entries[row] = list_entry_t(builder.row_children, builder.LocalChild());
		values_entries[row] = list_entry_t(builder.row_values, builder.LocalValue());
	}

	auto total_keys = builder.key_slots.size();
	auto total_children = builder.child_value_ids.size();
	auto total_values = builder.type_ids.size();

	//! Size the list child vectors now that the totals are known
	ListVector::Reserve(keys, total_keys);
	ListVector::SetListSize(keys, total_keys);
	ListVector::Reserve(children, total_children);
	ListVector::SetListSize(children, total_children);
	ListVector::Reserve(values, total_values);
	ListVector::SetListSize(values, total_values);

	VariantVectorData variant_data(result);
	for (idx_t row = 0; row < count; row++) {
		variant_data.keys_data[row] = keys_entries[row];
		variant_data.children_data[row] = children_entries[row];
		variant_data.values_data[row] = values_entries[row];
	}

	//! values
	if (total_values) {
		memcpy(variant_data.type_ids_data, builder.type_ids.data(), total_values * sizeof(uint8_t));
		memcpy(variant_data.byte_offset_data, builder.byte_offsets.data(), total_values * sizeof(uint32_t));
	}

	//! children
	for (idx_t i = 0; i < total_children; i++) {
		variant_data.values_index_data[i] = builder.child_value_ids[i];
		if (builder.child_key_ids[i] == INVALID_KEY) {
			variant_data.keys_index_validity.SetInvalid(i);
		} else {
			variant_data.keys_index_data[i] = builder.child_key_ids[i];
		}
	}

	//! keys: map each key slot to its (unsorted) dictionary index, then finalize (sort + remap)
	SelectionVector keys_selvec(total_keys);
	for (idx_t i = 0; i < total_keys; i++) {
		keys_selvec.set_index(i, builder.key_slots[i]);
	}
	VariantUtils::FinalizeVariantKeys(result, dictionary, keys_selvec, total_keys);
	keys_entry.Slice(keys_selvec, total_keys);

	FlatVector::SetSize(result, count);
	result.Verify();
}

} // namespace

void VariantValue::ToVARIANT(vector<VariantValue> &input, Vector &result) {
	VariantValueSource source(input);
	BuildVariant(source, input.size(), result);
}

void VariantValue::ToVARIANT(const VariantIterator &state, idx_t count, Vector &result) {
	VariantIteratorSource source(state);
	BuildVariant(source, count, result);
}

yyjson_mut_val *VariantValue::ToJSON(ClientContext &context, yyjson_mut_doc *doc) const {
	switch (value_type) {
	case VariantValueType::PRIMITIVE: {
		if (primitive_value.IsNull()) {
			return yyjson_mut_null(doc);
		}
		switch (primitive_value.type().id()) {
		case LogicalTypeId::BOOLEAN: {
			if (primitive_value.GetValue<bool>()) {
				return yyjson_mut_true(doc);
			} else {
				return yyjson_mut_false(doc);
			}
		}
		case LogicalTypeId::TINYINT:
			return yyjson_mut_int(doc, primitive_value.GetValue<int8_t>());
		case LogicalTypeId::SMALLINT:
			return yyjson_mut_int(doc, primitive_value.GetValue<int16_t>());
		case LogicalTypeId::INTEGER:
			return yyjson_mut_int(doc, primitive_value.GetValue<int32_t>());
		case LogicalTypeId::BIGINT:
			return yyjson_mut_int(doc, primitive_value.GetValue<int64_t>());
		case LogicalTypeId::FLOAT:
			return yyjson_mut_real(doc, primitive_value.GetValue<float>());
		case LogicalTypeId::DOUBLE:
			return yyjson_mut_real(doc, primitive_value.GetValue<double>());
		case LogicalTypeId::BLOB: {
			//! Follow the JSON serialization guide by converting BINARY to Base64:
			//! For example: `"dmFyaWFudAo="`
			auto value_str = Blob::ToBase64(primitive_value.GetValueUnsafe<string_t>());
			return yyjson_mut_strncpy(doc, value_str.c_str(), value_str.size());
		}
		case LogicalTypeId::DATE:
		case LogicalTypeId::TIME:
		case LogicalTypeId::VARCHAR: {
			auto value_str = primitive_value.ToString();
			return yyjson_mut_strncpy(doc, value_str.c_str(), value_str.size());
		}
		case LogicalTypeId::TIMESTAMP: {
			auto value_str = primitive_value.ToString();
			return yyjson_mut_strncpy(doc, value_str.c_str(), value_str.size());
		}
		case LogicalTypeId::TIMESTAMP_TZ: {
			auto value_str = primitive_value.CastAs(context, LogicalType::VARCHAR).GetValue<string>();
			return yyjson_mut_strncpy(doc, value_str.c_str(), value_str.size());
		}
		case LogicalTypeId::TIMESTAMP_TZ_NS: {
			auto value_str = primitive_value.CastAs(context, LogicalType::VARCHAR).GetValue<string>();
			return yyjson_mut_strncpy(doc, value_str.c_str(), value_str.size());
		}
		case LogicalTypeId::TIMESTAMP_NS: {
			auto value_str = primitive_value.CastAs(context, LogicalType::VARCHAR).GetValue<string>();
			return yyjson_mut_strncpy(doc, value_str.c_str(), value_str.size());
		}
		default:
			throw InternalException("Unexpected primitive type: %s", primitive_value.type().ToString());
		}
	}
	case VariantValueType::OBJECT: {
		auto obj = yyjson_mut_obj(doc);
		for (const auto &it : object_children) {
			auto &key = it.first;
			auto value = it.second.ToJSON(context, doc);
			yyjson_mut_obj_add_val(doc, obj, key.c_str(), value);
		}
		return obj;
	}
	case VariantValueType::ARRAY: {
		auto arr = yyjson_mut_arr(doc);
		for (auto &item : array_items) {
			auto value = item.ToJSON(context, doc);
			yyjson_mut_arr_add_val(arr, value);
		}
		return arr;
	}
	default:
		throw InternalException("Can't serialize this VariantValue type to JSON");
	}
}

} // namespace duckdb
