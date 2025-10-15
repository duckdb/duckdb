#include "duckdb/function/scalar/variant_utils.hpp"
#include "duckdb/function/scalar/variant_functions.hpp"
#include "duckdb/function/scalar/regexp.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/execution/expression_executor.hpp"

#include "duckdb/function/cast/variant/to_variant_fwd.hpp"
#include "duckdb/common/types/variant_visitor.hpp"

namespace duckdb {

namespace {

struct KeysVisitorState {
public:
	KeysVisitorState(uint64_t keys_offset, data_ptr_t blob_data, variant::ToVariantGlobalResultData &result_data)
	    : keys_offset(keys_offset), blob_data(blob_data), result_data(result_data) {
	}

public:
	data_ptr_t GetDestination() {
		return blob_data + blob_size;
	}

public:
	uint64_t keys_offset;

	idx_t keys_size = 0;
	idx_t children_size = 0;
	idx_t values_size = 0;
	uint32_t blob_size = 0;

	data_ptr_t blob_data;
	variant::ToVariantGlobalResultData &result_data;
};

//! Only cares about the keys of a VARIANT, maps which keys are reachable (i.e not orphaned by a 'variant_extract'
//! operation)
struct KeysVisitor {
	using result_type = void;

	static void VisitNull(KeysVisitorState &state) {
		state.values_size++;
		return;
	}
	static void VisitBoolean(bool, KeysVisitorState &state) {
		state.values_size++;
		return;
	}

	template <typename T>
	static void VisitInteger(T val, KeysVisitorState &state) {
		state.values_size++;
		Store<T>(val, state.GetDestination());
		state.blob_size += sizeof(T);
	}
	static void VisitFloat(float val, KeysVisitorState &state) {
		VisitInteger(val, state);
	}
	static void VisitDouble(double val, KeysVisitorState &state) {
		VisitInteger(val, state);
	}
	static void VisitUUID(hugeint_t val, KeysVisitorState &state) {
		VisitInteger(val, state);
	}
	static void VisitDate(date_t val, KeysVisitorState &state) {
		VisitInteger(val, state);
	}
	static void VisitInterval(interval_t val, KeysVisitorState &state) {
		VisitInteger(val, state);
	}
	static void VisitTime(dtime_t val, KeysVisitorState &state) {
		VisitInteger(val, state);
	}
	static void VisitTimeNanos(dtime_ns_t val, KeysVisitorState &state) {
		VisitInteger(val, state);
	}
	static void VisitTimeTZ(dtime_tz_t val, KeysVisitorState &state) {
		VisitInteger(val, state);
	}
	static void VisitTimestampSec(timestamp_sec_t val, KeysVisitorState &state) {
		VisitInteger(val, state);
	}
	static void VisitTimestampMs(timestamp_ms_t val, KeysVisitorState &state) {
		VisitInteger(val, state);
	}
	static void VisitTimestamp(timestamp_t val, KeysVisitorState &state) {
		VisitInteger(val, state);
	}
	static void VisitTimestampNanos(timestamp_ns_t val, KeysVisitorState &state) {
		VisitInteger(val, state);
	}
	static void VisitTimestampTZ(timestamp_tz_t val, KeysVisitorState &state) {
		VisitInteger(val, state);
	}

	static void VisitString(const string_t &str, KeysVisitorState &state) {
		state.values_size++;
		auto length = str.GetSize();
		state.blob_size += VarintEncode(length, state.GetDestination());
		memcpy(state.GetDestination(), str.GetData(), length);
		state.blob_size += length;
	}
	static void VisitBlob(const string_t &blob, KeysVisitorState &state) {
		return VisitString(blob, state);
	}
	static void VisitBignum(const string_t &bignum, KeysVisitorState &state) {
		return VisitString(bignum, state);
	}
	static void VisitGeometry(const string_t &geom, KeysVisitorState &state) {
		return VisitString(geom, state);
	}
	static void VisitBitstring(const string_t &bits, KeysVisitorState &state) {
		return VisitString(bits, state);
	}

	template <typename T>
	static void VisitDecimal(T val, uint32_t width, uint32_t scale, KeysVisitorState &state) {
		state.values_size++;
		state.blob_size += VarintEncode(width, state.GetDestination());
		state.blob_size += VarintEncode(scale, state.GetDestination());
		Store<T>(val, state.GetDestination());
		state.blob_size += sizeof(T);
	}

	static void VisitArray(const UnifiedVariantVectorData &variant, idx_t row, const VariantNestedData &nested_data,
	                       KeysVisitorState &state) {
		state.values_size++;
		state.blob_size += VarintEncode(nested_data.child_count, state.GetDestination());
		if (nested_data.child_count) {
			state.blob_size += VarintEncode(state.children_size, state.GetDestination());
			state.children_size += nested_data.child_count;

			VariantVisitor<KeysVisitor>::VisitArrayItems(variant, row, nested_data, state);
		}
	}

	static void VisitObject(const UnifiedVariantVectorData &variant, idx_t row, const VariantNestedData &nested_data,
	                        KeysVisitorState &state) {
		state.values_size++;
		state.blob_size += VarintEncode(nested_data.child_count, state.GetDestination());
		if (!nested_data.child_count) {
			return;
		}
		state.blob_size += VarintEncode(state.children_size, state.GetDestination());
		state.children_size += nested_data.child_count;

		//! First iterate through all fields to populate the map of key -> field
		map<string, idx_t> sorted_fields;
		for (idx_t i = 0; i < nested_data.child_count; i++) {
			auto keys_index = variant.GetKeysIndex(row, nested_data.children_idx + i);
			auto &key = variant.GetKey(row, keys_index);
			sorted_fields.emplace(key, i);
		}

		//! Then visit the fields in sorted order
		for (auto &entry : sorted_fields) {
			auto children_idx = nested_data.children_idx + entry.second;

			//! Add the key of the field to the result
			auto keys_index = variant.GetKeysIndex(row, children_idx);
			auto &key = variant.GetKey(row, keys_index);
			auto dict_index = state.result_data.GetOrCreateIndex(key);
			state.result_data.keys_selvec.set_index(state.keys_offset + state.keys_size, dict_index);

			//! Visit the child value
			auto values_index = variant.GetValuesIndex(row, children_idx);
			VariantVisitor<KeysVisitor>::Visit(variant, row, values_index, state);
		}
	}

	static void VisitDefault(VariantLogicalType type_id, const_data_ptr_t, KeysVisitorState &state) {
		throw InternalException("VariantLogicalType(%s) not handled", EnumUtil::ToString(type_id));
	}
};

} // namespace

static void VariantNormalizeFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	auto count = input.size();

	D_ASSERT(input.ColumnCount() == 1);
	auto &variant_vec = input.data[0];
	D_ASSERT(variant_vec.GetType() == LogicalType::VARIANT());
	auto &allocator = Allocator::DefaultAllocator();

	//! Set up the access helper for the source VARIANT
	RecursiveUnifiedVectorFormat source_format;
	Vector::RecursiveToUnifiedFormat(variant_vec, count, source_format);
	UnifiedVariantVectorData variant(source_format);

	//! Set up the state for populating the result VARIANT
	DataChunk offsets;
	offsets.Initialize(
	    allocator, {LogicalType::UINTEGER, LogicalType::UINTEGER, LogicalType::UINTEGER, LogicalType::UINTEGER}, count);
	offsets.SetCardinality(count);

	//! Take the original sizes of the lists, the result will be similar size, never bigger
	auto original_keys_size = ListVector::GetListSize(VariantVector::GetKeys(variant_vec));
	auto original_children_size = ListVector::GetListSize(VariantVector::GetChildren(variant_vec));
	auto original_values_size = ListVector::GetListSize(VariantVector::GetValues(variant_vec));

	auto &keys = VariantVector::GetKeys(result);
	auto &children = VariantVector::GetChildren(result);
	auto &values = VariantVector::GetValues(result);
	auto &data = VariantVector::GetData(result);

	ListVector::Reserve(keys, original_keys_size);
	ListVector::SetListSize(keys, 0);
	ListVector::Reserve(children, original_children_size);
	ListVector::SetListSize(children, 0);
	ListVector::Reserve(values, original_values_size);
	ListVector::SetListSize(values, 0);

	SelectionVector keys_selvec;
	keys_selvec.Initialize(original_keys_size);

	//! Initialize the dictionary
	auto &keys_entry = ListVector::GetEntry(keys);
	OrderedOwningStringMap<uint32_t> dictionary(StringVector::GetStringBuffer(keys_entry).GetStringAllocator());

	VariantVectorData variant_data(result);
	variant::ToVariantGlobalResultData result_variant(variant_data, offsets, dictionary, keys_selvec);

	auto result_data = FlatVector::GetData<string_t>(data);
	auto result_keys = FlatVector::GetData<list_entry_t>(keys);
	auto result_children = FlatVector::GetData<list_entry_t>(children);
	auto result_values = FlatVector::GetData<list_entry_t>(values);
	for (idx_t i = 0; i < count; i++) {
		if (!variant.RowIsValid(i)) {
			FlatVector::SetNull(result, i, true);
			continue;
		}
		//! Allocate for the new data, use the same size as source
		auto &blob_data = result_data[i];
		auto original_data = variant.GetData(i);
		blob_data = StringVector::EmptyString(data, original_data.GetSize());

		auto &keys_list_entry = result_keys[i];
		keys_list_entry.offset = ListVector::GetListSize(keys);

		auto &children_list_entry = result_children[i];
		children_list_entry.offset = ListVector::GetListSize(children);

		auto &values_list_entry = result_values[i];
		values_list_entry.offset = ListVector::GetListSize(values);

		//! Visit the source to populate the result
		KeysVisitorState visitor_state(keys_list_entry.offset, data_ptr_cast(blob_data.GetDataWriteable()),
		                               result_variant);
		VariantVisitor<KeysVisitor>::Visit(variant, i, 0, visitor_state);

		blob_data.SetSizeAndFinalize(visitor_state.blob_size, original_data.GetSize());
		keys_list_entry.length = visitor_state.keys_size;
		children_list_entry.length = visitor_state.children_size;
		values_list_entry.length = visitor_state.values_size;

		ListVector::SetListSize(keys, ListVector::GetListSize(keys) + visitor_state.keys_size);
		ListVector::SetListSize(children, ListVector::GetListSize(children) + visitor_state.children_size);
		ListVector::SetListSize(values, ListVector::GetListSize(values) + visitor_state.values_size);
	}

	if (input.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

ScalarFunction VariantNormalizeFun::GetFunction() {
	auto variant_type = LogicalType::VARIANT();
	return ScalarFunction("variant_normalize", {variant_type}, variant_type, VariantNormalizeFunction);
}

} // namespace duckdb
