//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/variant/variant_builder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/vector_writer.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/common/vector/variant_vector.hpp"
#include "duckdb/common/types/variant_value.hpp"
#include "duckdb/common/types/variant_iterator.hpp"
#include "duckdb/common/serializer/varint.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/datetime.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/variant.hpp"
#include "duckdb/common/hugeint.hpp"
#include "duckdb/function/scalar/variant_utils.hpp"
#include "duckdb/common/string_map_set.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/owning_string_map.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/limits.hpp"

#include <type_traits>

namespace duckdb {

//===--------------------------------------------------------------------===//
// Building a VARIANT in a single pass
//===--------------------------------------------------------------------===//
// The canonical (unshredded) VARIANT layout is built directly while traversing the source tree once.
// Rather than a separate "analyze sizes" pass followed by an in-place "convert" pass, the blob bytes
// and the (values / children / keys) entries are accumulated into growable buffers, and copied into
// the result vectors once the per-row sizes are known. The data is sourced either from a
// vector<VariantValue>, a VariantIterator (unshredding), or a ParquetVariantIterator (the parquet reader).

//! Sentinel marking an array child (whose 'key_id' is NULL)
constexpr uint32_t VARIANT_INVALID_KEY = NumericLimits<uint32_t>::Maximum();

inline void VariantBuilderAppendVarint(string &blob, uint32_t value) {
	auto size = GetVarintSize(value);
	auto pos = blob.size();
	blob.resize(pos + size);
	VarintEncode<uint32_t>(value, data_ptr_cast(blob.data()) + pos);
}

template <class T>
void VariantBuilderAppendFixed(string &blob, T value) {
	auto pos = blob.size();
	blob.resize(pos + sizeof(T));
	Store<T>(value, data_ptr_cast(blob.data()) + pos);
}

inline void VariantBuilderAppendBytes(string &blob, const_data_ptr_t data, idx_t size) {
	blob.append(const_char_ptr_cast(data), size);
}

inline uint32_t VariantBuilderGetOrCreateIndex(OrderedOwningStringMap<uint32_t> &dictionary, const string_t &key) {
	auto unsorted_idx = dictionary.size();
	//! This will later be remapped to the sorted idx (see FinalizeVariantKeys in 'to_variant.cpp')
	return dictionary.emplace(std::make_pair(key, unsorted_idx)).first->second;
}

//! The (fixed) blob payload size of a fixed-width primitive VariantLogicalType
inline idx_t VariantFixedPayloadSize(VariantLogicalType type_id) {
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
inline bool VariantIsStringType(VariantLogicalType type_id) {
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

//! Accumulates the canonical representation of a single chunk while traversing the source once.
struct VariantBuilder {
	explicit VariantBuilder(OrderedOwningStringMap<uint32_t> &dictionary) : dictionary(dictionary) {
	}

	//! values: (type_id, byte_offset) in pre-order
	vector<uint8_t> type_ids;
	vector<uint32_t> byte_offsets;
	//! children: (key_id, value_id) - key_id is VARIANT_INVALID_KEY for array elements
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

	//! Emit a VARIANT_NULL value
	void EmitNull() {
		type_ids.push_back(static_cast<uint8_t>(VariantLogicalType::VARIANT_NULL));
		byte_offsets.push_back(NumericCast<uint32_t>(blob.size()));
	}

	//! Emit an OBJECT value with 'n' children (assumed to be in lexicographic key order). 'key_fn(i)'
	//! returns the (string_t) key of child i; 'emit_fn(i)' must emit exactly one value for child i.
	template <class KEY_FN, class EMIT_FN>
	void EmitObject(idx_t n, KEY_FN &&key_fn, EMIT_FN &&emit_fn) {
		auto byte_offset = NumericCast<uint32_t>(blob.size());
		type_ids.push_back(static_cast<uint8_t>(VariantLogicalType::OBJECT));
		byte_offsets.push_back(byte_offset);
		VariantBuilderAppendVarint(blob, NumericCast<uint32_t>(n));
		if (!n) {
			return;
		}
		VariantBuilderAppendVarint(blob, LocalChild());
		auto block = child_value_ids.size();
		child_value_ids.resize(block + n);
		child_key_ids.resize(block + n);
		for (idx_t i = 0; i < n; i++) {
			child_value_ids[block + i] = LocalValue();
			child_key_ids[block + i] = LocalKey();
			key_slots.push_back(VariantBuilderGetOrCreateIndex(dictionary, key_fn(i)));
			emit_fn(i);
		}
	}

	//! Emit an ARRAY value with 'n' elements. 'emit_fn(i)' must emit exactly one value for element i.
	template <class EMIT_FN>
	void EmitArray(idx_t n, EMIT_FN &&emit_fn) {
		auto byte_offset = NumericCast<uint32_t>(blob.size());
		type_ids.push_back(static_cast<uint8_t>(VariantLogicalType::ARRAY));
		byte_offsets.push_back(byte_offset);
		VariantBuilderAppendVarint(blob, NumericCast<uint32_t>(n));
		if (!n) {
			return;
		}
		VariantBuilderAppendVarint(blob, LocalChild());
		auto block = child_value_ids.size();
		child_value_ids.resize(block + n);
		child_key_ids.resize(block + n);
		for (idx_t i = 0; i < n; i++) {
			child_value_ids[block + i] = LocalValue();
			child_key_ids[block + i] = VARIANT_INVALID_KEY;
			emit_fn(i);
		}
	}

	//! Emit a plain (non-nested) Value as a primitive variant value
	void EmitPrimitive(const Value &primitive, uint32_t byte_offset) {
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
			VariantBuilderAppendFixed(blob, primitive.GetValueUnsafe<int8_t>());
			break;
		case LogicalTypeId::SMALLINT:
			variant_type = VariantLogicalType::INT16;
			VariantBuilderAppendFixed(blob, primitive.GetValueUnsafe<int16_t>());
			break;
		case LogicalTypeId::INTEGER:
			variant_type = VariantLogicalType::INT32;
			VariantBuilderAppendFixed(blob, primitive.GetValueUnsafe<int32_t>());
			break;
		case LogicalTypeId::BIGINT:
			variant_type = VariantLogicalType::INT64;
			VariantBuilderAppendFixed(blob, primitive.GetValueUnsafe<int64_t>());
			break;
		case LogicalTypeId::HUGEINT:
			variant_type = VariantLogicalType::INT128;
			VariantBuilderAppendFixed(blob, primitive.GetValueUnsafe<hugeint_t>());
			break;
		case LogicalTypeId::UTINYINT:
			variant_type = VariantLogicalType::UINT8;
			VariantBuilderAppendFixed(blob, primitive.GetValueUnsafe<uint8_t>());
			break;
		case LogicalTypeId::USMALLINT:
			variant_type = VariantLogicalType::UINT16;
			VariantBuilderAppendFixed(blob, primitive.GetValueUnsafe<uint16_t>());
			break;
		case LogicalTypeId::UINTEGER:
			variant_type = VariantLogicalType::UINT32;
			VariantBuilderAppendFixed(blob, primitive.GetValueUnsafe<uint32_t>());
			break;
		case LogicalTypeId::UBIGINT:
			variant_type = VariantLogicalType::UINT64;
			VariantBuilderAppendFixed(blob, primitive.GetValueUnsafe<uint64_t>());
			break;
		case LogicalTypeId::UHUGEINT:
			variant_type = VariantLogicalType::UINT128;
			VariantBuilderAppendFixed(blob, primitive.GetValueUnsafe<uhugeint_t>());
			break;
		case LogicalTypeId::DOUBLE:
			variant_type = VariantLogicalType::DOUBLE;
			VariantBuilderAppendFixed(blob, primitive.GetValueUnsafe<double>());
			break;
		case LogicalTypeId::FLOAT:
			variant_type = VariantLogicalType::FLOAT;
			VariantBuilderAppendFixed(blob, primitive.GetValueUnsafe<float>());
			break;
		case LogicalTypeId::DATE:
			variant_type = VariantLogicalType::DATE;
			VariantBuilderAppendFixed(blob, primitive.GetValueUnsafe<date_t>());
			break;
		case LogicalTypeId::TIMESTAMP_TZ:
			variant_type = VariantLogicalType::TIMESTAMP_MICROS_TZ;
			VariantBuilderAppendFixed(blob, primitive.GetValueUnsafe<timestamp_tz_t>());
			break;
		case LogicalTypeId::TIMESTAMP_TZ_NS:
			variant_type = VariantLogicalType::TIMESTAMP_NANOS_TZ;
			VariantBuilderAppendFixed(blob, primitive.GetValueUnsafe<timestamp_tz_ns_t>());
			break;
		case LogicalTypeId::TIMESTAMP:
			variant_type = VariantLogicalType::TIMESTAMP_MICROS;
			VariantBuilderAppendFixed(blob, primitive.GetValueUnsafe<timestamp_t>());
			break;
		case LogicalTypeId::TIMESTAMP_SEC:
			variant_type = VariantLogicalType::TIMESTAMP_SEC;
			VariantBuilderAppendFixed(blob, primitive.GetValueUnsafe<timestamp_sec_t>());
			break;
		case LogicalTypeId::TIMESTAMP_MS:
			variant_type = VariantLogicalType::TIMESTAMP_MILIS;
			VariantBuilderAppendFixed(blob, primitive.GetValueUnsafe<timestamp_ms_t>());
			break;
		case LogicalTypeId::TIME:
			variant_type = VariantLogicalType::TIME_MICROS;
			VariantBuilderAppendFixed(blob, primitive.GetValueUnsafe<dtime_t>());
			break;
		case LogicalTypeId::TIME_NS:
			variant_type = VariantLogicalType::TIME_NANOS;
			VariantBuilderAppendFixed(blob, primitive.GetValueUnsafe<dtime_ns_t>());
			break;
		case LogicalTypeId::TIME_TZ:
			variant_type = VariantLogicalType::TIME_MICROS_TZ;
			VariantBuilderAppendFixed(blob, primitive.GetValueUnsafe<dtime_tz_t>());
			break;
		case LogicalTypeId::TIMESTAMP_NS:
			variant_type = VariantLogicalType::TIMESTAMP_NANOS;
			VariantBuilderAppendFixed(blob, primitive.GetValueUnsafe<timestamp_ns_t>());
			break;
		case LogicalTypeId::INTERVAL:
			variant_type = VariantLogicalType::INTERVAL;
			VariantBuilderAppendFixed(blob, primitive.GetValueUnsafe<interval_t>());
			break;
		case LogicalTypeId::UUID:
			variant_type = VariantLogicalType::UUID;
			VariantBuilderAppendFixed(blob, primitive.GetValueUnsafe<hugeint_t>());
			break;
		case LogicalTypeId::DECIMAL: {
			variant_type = VariantLogicalType::DECIMAL;
			auto &type = primitive.type();
			uint8_t width;
			uint8_t scale;
			type.GetDecimalProperties(width, scale);
			VariantBuilderAppendVarint(blob, width);
			VariantBuilderAppendVarint(blob, scale);
			switch (type.InternalType()) {
			case PhysicalType::INT16:
				VariantBuilderAppendFixed(blob, primitive.GetValueUnsafe<int16_t>());
				break;
			case PhysicalType::INT32:
				VariantBuilderAppendFixed(blob, primitive.GetValueUnsafe<int32_t>());
				break;
			case PhysicalType::INT64:
				VariantBuilderAppendFixed(blob, primitive.GetValueUnsafe<int64_t>());
				break;
			case PhysicalType::INT128:
				VariantBuilderAppendFixed(blob, primitive.GetValueUnsafe<hugeint_t>());
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
			VariantBuilderAppendVarint(blob, NumericCast<uint32_t>(string_data.GetSize()));
			VariantBuilderAppendBytes(blob, const_data_ptr_cast(string_data.GetData()), string_data.GetSize());
			break;
		}
		default:
			throw InternalException("Encountered unrecognized LogicalType in EmitPrimitive: %s",
			                        primitive.type().ToString());
		}
		type_ids.push_back(static_cast<uint8_t>(variant_type));
		byte_offsets.push_back(byte_offset);
	}

	//! Emit a whole VariantValue subtree (a decoded / materialized variant value)
	void EmitVariantValue(const VariantValue &value) {
		switch (value.value_type) {
		case VariantValueType::OBJECT: {
			auto &children = value.ObjectChildren();
			//! The children map is ordered by key, gather pointers for indexed access
			vector<const std::pair<const string, VariantValue> *> entries;
			entries.reserve(children.size());
			for (auto &entry : children) {
				entries.push_back(&entry);
			}
			EmitObject(
			    entries.size(), [&](idx_t i) { return string_t(entries[i]->first); },
			    [&](idx_t i) { EmitVariantValue(entries[i]->second); });
			break;
		}
		case VariantValueType::ARRAY: {
			auto &items = value.ArrayItems();
			EmitArray(items.size(), [&](idx_t i) { EmitVariantValue(items[i]); });
			break;
		}
		case VariantValueType::PRIMITIVE:
			EmitPrimitive(value.primitive_value, NumericCast<uint32_t>(blob.size()));
			break;
		default:
			throw InternalException("VariantValueType not handled");
		}
	}
};

//===--------------------------------------------------------------------===//
// Emit (source: a VariantNode-like cursor)
//===--------------------------------------------------------------------===//
//! Collect the (non-missing) object children of a node in lexicographic key order
template <class NODE>
auto CollectObjectChildren(const NODE &it) {
	auto object = it.GetObjectChildren(VariantIterationOrder::LEXICOGRAPHIC);
	using EntryT = std::decay_t<decltype(*object.begin())>;
	vector<EntryT> children;
	for (auto &entry : object) {
		children.push_back(entry);
	}
	return children;
}

//! Traverse a VariantNode-like cursor 'it' (any type exposing the node concept) into the builder.
template <class NODE>
void EmitIterator(const NODE &it, VariantBuilder &builder) {
	if (it.IsNull() || it.IsMissing()) {
		builder.EmitNull();
		return;
	}

	auto type_id = it.GetTypeId();
	switch (type_id) {
	case VariantLogicalType::OBJECT: {
		auto children = CollectObjectChildren(it);
		builder.EmitObject(
		    children.size(), [&](idx_t i) { return children[i].key; },
		    [&](idx_t i) { EmitIterator(children[i].value, builder); });
		break;
	}
	case VariantLogicalType::ARRAY: {
		auto array = it.GetArrayChildren();
		builder.EmitArray(array.size(), [&](idx_t i) { EmitIterator(array[i], builder); });
		break;
	}
	case VariantLogicalType::DECIMAL: {
		auto decimal = it.GetDecimal();
		auto byte_offset = NumericCast<uint32_t>(builder.blob.size());
		builder.type_ids.push_back(static_cast<uint8_t>(VariantLogicalType::DECIMAL));
		builder.byte_offsets.push_back(byte_offset);
		VariantBuilderAppendVarint(builder.blob, decimal.width);
		VariantBuilderAppendVarint(builder.blob, decimal.scale);
		VariantBuilderAppendBytes(builder.blob, decimal.value_ptr, GetTypeIdSize(decimal.GetPhysicalType()));
		break;
	}
	default: {
		auto byte_offset = NumericCast<uint32_t>(builder.blob.size());
		builder.type_ids.push_back(static_cast<uint8_t>(type_id));
		builder.byte_offsets.push_back(byte_offset);
		if (VariantIsStringType(type_id)) {
			auto str = it.GetString();
			VariantBuilderAppendVarint(builder.blob, NumericCast<uint32_t>(str.GetSize()));
			VariantBuilderAppendBytes(builder.blob, const_data_ptr_cast(str.GetData()), str.GetSize());
		} else {
			auto size = VariantFixedPayloadSize(type_id);
			if (size) {
				VariantBuilderAppendBytes(builder.blob, it.GetDataPointer(), size);
			}
		}
		break;
	}
	}
}

//===--------------------------------------------------------------------===//
// Build driver
//===--------------------------------------------------------------------===//
//! Build the canonical (unshredded) VARIANT 'result' vector for 'count' rows by emitting each row of
//! 'source' (which provides 'bool Emit(idx_t row, VariantBuilder &builder)' returning whether the row
//! is a SQL NULL) into a shared VariantBuilder, then materializing the accumulated buffers.
template <class SOURCE>
void BuildVariant(SOURCE &source, idx_t count, Vector &result) {
	if (count == 0) {
		return;
	}

	auto &keys = VariantVector::GetKeys(result);
	auto &keys_entry = ListVector::GetChildMutable(keys);
	auto &children = VariantVector::GetChildren(result);
	auto &values = VariantVector::GetValues(result);
	auto &blob_vector = VariantVector::GetData(result);
	auto blob_writer = FlatVector::Writer<string_t>(blob_vector, count);

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
		blob_writer.WriteValue(string_t(builder.blob.data(), NumericCast<uint32_t>(builder.blob.size())));
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
		if (builder.child_key_ids[i] == VARIANT_INVALID_KEY) {
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

} // namespace duckdb
