#include "duckdb/function/scalar/variant_utils.hpp"
#include "duckdb/function/scalar/variant_functions.hpp"
#include "duckdb/function/scalar/regexp.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/execution/expression_executor.hpp"

#include "duckdb/function/cast/variant/to_variant_fwd.hpp"
#include "duckdb/common/types/variant_visitor.hpp"

namespace duckdb {

namespace {

struct VariantNormalizerState {
public:
	VariantNormalizerState(idx_t result_row, VariantVectorData &source, OrderedOwningStringMap<uint32_t> &dictionary,
	                       SelectionVector &keys_selvec)
	    : source(source), dictionary(dictionary), keys_selvec(keys_selvec),
	      keys_index_validity(source.keys_index_validity) {
		auto keys_list_entry = source.keys_data[result_row];
		auto values_list_entry = source.values_data[result_row];
		auto children_list_entry = source.children_data[result_row];

		keys_offset = keys_list_entry.offset;
		children_offset = children_list_entry.offset;

		blob_data = data_ptr_cast(source.blob_data[result_row].GetDataWriteable());
		type_ids = source.type_ids_data + values_list_entry.offset;
		byte_offsets = source.byte_offset_data + values_list_entry.offset;
		values_indexes = source.values_index_data + children_list_entry.offset;
		keys_indexes = source.keys_index_data + children_list_entry.offset;
	}

public:
	data_ptr_t GetDestination() {
		return blob_data + blob_size;
	}
	uint32_t GetOrCreateIndex(const string_t &key) {
		auto unsorted_idx = dictionary.size();
		//! This will later be remapped to the sorted idx (see FinalizeVariantKeys in 'to_variant.cpp')
		return dictionary.emplace(std::make_pair(key, unsorted_idx)).first->second;
	}

public:
	uint32_t keys_size = 0;
	uint32_t children_size = 0;
	uint32_t values_size = 0;
	uint32_t blob_size = 0;

	VariantVectorData &source;
	OrderedOwningStringMap<uint32_t> &dictionary;
	SelectionVector &keys_selvec;

	uint64_t keys_offset;
	uint64_t children_offset;
	ValidityMask &keys_index_validity;

	data_ptr_t blob_data;
	uint8_t *type_ids;
	uint32_t *byte_offsets;
	uint32_t *values_indexes;
	uint32_t *keys_indexes;
};

struct VariantNormalizer {
	using result_type = void;

	static void VisitNull(VariantNormalizerState &state) {
		return;
	}
	static void VisitBoolean(bool val, VariantNormalizerState &state) {
		return;
	}

	static void VisitMetadata(VariantLogicalType type_id, VariantNormalizerState &state) {
		state.type_ids[state.values_size] = static_cast<uint8_t>(type_id);
		state.byte_offsets[state.values_size] = state.blob_size;
		state.values_size++;
	}

	template <typename T>
	static void VisitInteger(T val, VariantNormalizerState &state) {
		Store<T>(val, state.GetDestination());
		state.blob_size += sizeof(T);
	}
	static void VisitFloat(float val, VariantNormalizerState &state) {
		VisitInteger(val, state);
	}
	static void VisitDouble(double val, VariantNormalizerState &state) {
		VisitInteger(val, state);
	}
	static void VisitUUID(hugeint_t val, VariantNormalizerState &state) {
		VisitInteger(val, state);
	}
	static void VisitDate(date_t val, VariantNormalizerState &state) {
		VisitInteger(val, state);
	}
	static void VisitInterval(interval_t val, VariantNormalizerState &state) {
		VisitInteger(val, state);
	}
	static void VisitTime(dtime_t val, VariantNormalizerState &state) {
		VisitInteger(val, state);
	}
	static void VisitTimeNanos(dtime_ns_t val, VariantNormalizerState &state) {
		VisitInteger(val, state);
	}
	static void VisitTimeTZ(dtime_tz_t val, VariantNormalizerState &state) {
		VisitInteger(val, state);
	}
	static void VisitTimestampSec(timestamp_sec_t val, VariantNormalizerState &state) {
		VisitInteger(val, state);
	}
	static void VisitTimestampMs(timestamp_ms_t val, VariantNormalizerState &state) {
		VisitInteger(val, state);
	}
	static void VisitTimestamp(timestamp_t val, VariantNormalizerState &state) {
		VisitInteger(val, state);
	}
	static void VisitTimestampNanos(timestamp_ns_t val, VariantNormalizerState &state) {
		VisitInteger(val, state);
	}
	static void VisitTimestampTZ(timestamp_tz_t val, VariantNormalizerState &state) {
		VisitInteger(val, state);
	}

	static void WriteStringInternal(const string_t &str, VariantNormalizerState &state) {
	}

	static void VisitString(const string_t &str, VariantNormalizerState &state) {
		auto length = str.GetSize();
		state.blob_size += VarintEncode(length, state.GetDestination());
		memcpy(state.GetDestination(), str.GetData(), length);
		state.blob_size += length;
	}
	static void VisitBlob(const string_t &blob, VariantNormalizerState &state) {
		return VisitString(blob, state);
	}
	static void VisitBignum(const string_t &bignum, VariantNormalizerState &state) {
		return VisitString(bignum, state);
	}
	static void VisitGeometry(const string_t &geom, VariantNormalizerState &state) {
		return VisitString(geom, state);
	}
	static void VisitBitstring(const string_t &bits, VariantNormalizerState &state) {
		return VisitString(bits, state);
	}

	template <typename T>
	static void VisitDecimal(T val, uint32_t width, uint32_t scale, VariantNormalizerState &state) {
		state.blob_size += VarintEncode(width, state.GetDestination());
		state.blob_size += VarintEncode(scale, state.GetDestination());
		Store<T>(val, state.GetDestination());
		state.blob_size += sizeof(T);
	}

	static void VisitArray(const UnifiedVariantVectorData &variant, idx_t row, const VariantNestedData &nested_data,
	                       VariantNormalizerState &state) {
		state.blob_size += VarintEncode(nested_data.child_count, state.GetDestination());
		if (!nested_data.child_count) {
			return;
		}
		idx_t result_children_idx = state.children_size;
		state.blob_size += VarintEncode(result_children_idx, state.GetDestination());
		state.children_size += nested_data.child_count;

		for (idx_t i = 0; i < nested_data.child_count; i++) {
			auto source_children_idx = nested_data.children_idx + i;
			auto values_index = variant.GetValuesIndex(row, source_children_idx);

			//! Set the 'values_index' for the child, and set the 'keys_index' to NULL
			state.values_indexes[result_children_idx] = state.values_size;
			state.keys_index_validity.SetInvalid(state.children_offset + result_children_idx);
			result_children_idx++;

			//! Visit the child value
			VariantVisitor<VariantNormalizer>::Visit(variant, row, values_index, state);
		}
	}

	static void VisitObject(const UnifiedVariantVectorData &variant, idx_t row, const VariantNestedData &nested_data,
	                        VariantNormalizerState &state) {
		state.blob_size += VarintEncode(nested_data.child_count, state.GetDestination());
		if (!nested_data.child_count) {
			return;
		}
		uint32_t children_idx = state.children_size;
		uint32_t keys_idx = state.keys_size;
		state.blob_size += VarintEncode(children_idx, state.GetDestination());
		state.children_size += nested_data.child_count;
		state.keys_size += nested_data.child_count;

		//! First iterate through all fields to populate the map of key -> field
		map<string, idx_t> sorted_fields;
		for (idx_t i = 0; i < nested_data.child_count; i++) {
			auto keys_index = variant.GetKeysIndex(row, nested_data.children_idx + i);
			auto &key = variant.GetKey(row, keys_index);
			sorted_fields.emplace(key, i);
		}

		//! Then visit the fields in sorted order
		for (auto &entry : sorted_fields) {
			auto source_children_idx = nested_data.children_idx + entry.second;

			//! Add the key of the field to the result
			auto keys_index = variant.GetKeysIndex(row, source_children_idx);
			auto &key = variant.GetKey(row, keys_index);
			auto dict_index = state.GetOrCreateIndex(key);
			state.keys_selvec.set_index(state.keys_offset + keys_idx, dict_index);

			//! Visit the child value
			auto values_index = variant.GetValuesIndex(row, source_children_idx);
			state.values_indexes[children_idx] = state.values_size;
			state.keys_indexes[children_idx] = keys_idx;
			children_idx++;
			keys_idx++;
			VariantVisitor<VariantNormalizer>::Visit(variant, row, values_index, state);
		}
	}

	static void VisitDefault(VariantLogicalType type_id, const_data_ptr_t, VariantNormalizerState &state) {
		throw InternalException("VariantLogicalType(%s) not handled", EnumUtil::ToString(type_id));
	}
};

} // namespace

static void VariantNormalizeFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	auto count = input.size();

	D_ASSERT(input.ColumnCount() == 1);
	auto &variant_vec = input.data[0];
	D_ASSERT(variant_vec.GetType() == LogicalType::VARIANT());

	//! Set up the access helper for the source VARIANT
	RecursiveUnifiedVectorFormat source_format;
	Vector::RecursiveToUnifiedFormat(variant_vec, count, source_format);
	UnifiedVariantVectorData variant(source_format);

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

	//! Initialize the dictionary
	auto &keys_entry = ListVector::GetEntry(keys);
	OrderedOwningStringMap<uint32_t> dictionary(StringVector::GetStringBuffer(keys_entry).GetStringAllocator());

	VariantVectorData variant_data(result);
	SelectionVector keys_selvec;
	keys_selvec.Initialize(original_keys_size);

	for (idx_t i = 0; i < count; i++) {
		if (!variant.RowIsValid(i)) {
			FlatVector::SetNull(result, i, true);
			continue;
		}
		//! Allocate for the new data, use the same size as source
		auto &blob_data = variant_data.blob_data[i];
		auto original_data = variant.GetData(i);
		blob_data = StringVector::EmptyString(data, original_data.GetSize());

		auto &keys_list_entry = variant_data.keys_data[i];
		keys_list_entry.offset = ListVector::GetListSize(keys);

		auto &children_list_entry = variant_data.children_data[i];
		children_list_entry.offset = ListVector::GetListSize(children);

		auto &values_list_entry = variant_data.values_data[i];
		values_list_entry.offset = ListVector::GetListSize(values);

		//! Visit the source to populate the result
		VariantNormalizerState visitor_state(i, variant_data, dictionary, keys_selvec);
		VariantVisitor<VariantNormalizer>::Visit(variant, i, 0, visitor_state);

		blob_data.SetSizeAndFinalize(visitor_state.blob_size, original_data.GetSize());
		keys_list_entry.length = visitor_state.keys_size;
		children_list_entry.length = visitor_state.children_size;
		values_list_entry.length = visitor_state.values_size;

		ListVector::SetListSize(keys, ListVector::GetListSize(keys) + visitor_state.keys_size);
		ListVector::SetListSize(children, ListVector::GetListSize(children) + visitor_state.children_size);
		ListVector::SetListSize(values, ListVector::GetListSize(values) + visitor_state.values_size);
	}

	VariantUtils::FinalizeVariantKeys(result, dictionary, keys_selvec, ListVector::GetListSize(keys));
	keys_entry.Slice(keys_selvec, ListVector::GetListSize(keys));

	if (input.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
	result.Verify(count);
}

ScalarFunction VariantNormalizeFun::GetFunction() {
	auto variant_type = LogicalType::VARIANT();
	return ScalarFunction("variant_normalize", {variant_type}, variant_type, VariantNormalizeFunction);
}

} // namespace duckdb
