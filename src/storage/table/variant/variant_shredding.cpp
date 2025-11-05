#include "duckdb/storage/table/variant_column_data.hpp"
#include "duckdb/common/types/variant.hpp"
#include "duckdb/common/types/variant_visitor.hpp"
#include "duckdb/function/variant/variant_shredding.hpp"
#include "duckdb/function/variant/variant_normalize.hpp"
#include "duckdb/common/serializer/varint.hpp"
#ifdef DEBUG
#include "duckdb/common/value_operations/value_operations.hpp"
#endif

namespace duckdb {

namespace {

static unordered_set<VariantLogicalType> GetVariantType(const LogicalType &type) {
	if (type.id() == LogicalTypeId::ANY) {
		return {};
	}
	switch (type.id()) {
	case LogicalTypeId::STRUCT:
		return {VariantLogicalType::OBJECT};
	case LogicalTypeId::LIST:
		return {VariantLogicalType::ARRAY};
	case LogicalTypeId::BOOLEAN:
		return {VariantLogicalType::BOOL_TRUE, VariantLogicalType::BOOL_FALSE};
	case LogicalTypeId::TINYINT:
		return {VariantLogicalType::INT8};
	case LogicalTypeId::SMALLINT:
		return {VariantLogicalType::INT16};
	case LogicalTypeId::INTEGER:
		return {VariantLogicalType::INT32};
	case LogicalTypeId::BIGINT:
		return {VariantLogicalType::INT64};
	case LogicalTypeId::HUGEINT:
		return {VariantLogicalType::INT128};
	case LogicalTypeId::UTINYINT:
		return {VariantLogicalType::UINT8};
	case LogicalTypeId::USMALLINT:
		return {VariantLogicalType::UINT16};
	case LogicalTypeId::UINTEGER:
		return {VariantLogicalType::UINT32};
	case LogicalTypeId::UBIGINT:
		return {VariantLogicalType::UINT64};
	case LogicalTypeId::UHUGEINT:
		return {VariantLogicalType::UINT128};
	case LogicalTypeId::FLOAT:
		return {VariantLogicalType::FLOAT};
	case LogicalTypeId::DOUBLE:
		return {VariantLogicalType::DOUBLE};
	case LogicalTypeId::DECIMAL:
		return {VariantLogicalType::DECIMAL};
	case LogicalTypeId::DATE:
		return {VariantLogicalType::DATE};
	case LogicalTypeId::TIME:
		return {VariantLogicalType::TIME_MICROS};
	case LogicalTypeId::TIME_TZ:
		return {VariantLogicalType::TIME_MICROS_TZ};
	case LogicalTypeId::TIMESTAMP_TZ:
		return {VariantLogicalType::TIMESTAMP_MICROS_TZ};
	case LogicalTypeId::TIMESTAMP:
		return {VariantLogicalType::TIMESTAMP_MICROS};
	case LogicalTypeId::TIMESTAMP_SEC:
		return {VariantLogicalType::TIMESTAMP_SEC};
	case LogicalTypeId::TIMESTAMP_MS:
		return {VariantLogicalType::TIMESTAMP_MILIS};
	case LogicalTypeId::TIMESTAMP_NS:
		return {VariantLogicalType::TIMESTAMP_NANOS};
	case LogicalTypeId::BLOB:
		return {VariantLogicalType::BLOB};
	case LogicalTypeId::VARCHAR:
		return {VariantLogicalType::VARCHAR};
	case LogicalTypeId::UUID:
		return {VariantLogicalType::UUID};
	case LogicalTypeId::BIGNUM:
		return {VariantLogicalType::BIGNUM};
	case LogicalTypeId::TIME_NS:
		return {VariantLogicalType::TIME_NANOS};
	case LogicalTypeId::INTERVAL:
		return {VariantLogicalType::INTERVAL};
	case LogicalTypeId::BIT:
		return {VariantLogicalType::BITSTRING};
	case LogicalTypeId::GEOMETRY:
		return {VariantLogicalType::GEOMETRY};
	default:
		throw BinderException("Type '%s' can't be translated to a VARIANT type", type.ToString());
	}
}

struct DuckDBVariantShreddingState : public VariantShreddingState {
public:
	DuckDBVariantShreddingState(const LogicalType &type, idx_t total_count)
	    : VariantShreddingState(type, total_count), variant_types(GetVariantType(type)) {
	}

public:
	const unordered_set<VariantLogicalType> &GetVariantTypes() override {
		return variant_types;
	}

private:
	unordered_set<VariantLogicalType> variant_types;
};

struct UnshreddedValue {
public:
	UnshreddedValue(uint32_t value_index, uint32_t &target_value_index, vector<uint32_t> &&children = {})
	    : source_value_index(value_index), target_value_index(target_value_index),
	      unshredded_children(std::move(children)) {
	}

public:
	uint32_t source_value_index;
	uint32_t &target_value_index;
	vector<uint32_t> unshredded_children;
};

struct DuckDBVariantShredding : public VariantShredding {
public:
	explicit DuckDBVariantShredding(idx_t count) : VariantShredding(), unshredded_values(count) {
	}
	~DuckDBVariantShredding() override = default;

public:
	void WriteVariantValues(UnifiedVariantVectorData &variant, Vector &result, optional_ptr<const SelectionVector> sel,
	                        optional_ptr<const SelectionVector> value_index_sel,
	                        optional_ptr<const SelectionVector> result_sel, idx_t count) override;
	void AnalyzeVariantValues(UnifiedVariantVectorData &variant, Vector &value, optional_ptr<const SelectionVector> sel,
	                          optional_ptr<const SelectionVector> value_index_sel,
	                          optional_ptr<const SelectionVector> result_sel,
	                          DuckDBVariantShreddingState &shredding_state, idx_t count);

public:
	//! For each row of the variant, the value_index(es) of the values to write to the 'unshredded' Vector
	vector<vector<UnshreddedValue>> unshredded_values;
};

} // namespace

static void VisitObject(const UnifiedVariantVectorData &variant, idx_t row, const VariantNestedData &nested_data,
                        VariantNormalizerState &state, const vector<uint32_t> &child_indices) {
	D_ASSERT(child_indices.size() <= nested_data.child_count);
	//! First iterate through all fields to populate the map of key -> field
	map<string, idx_t> sorted_fields;
	for (auto &child_idx : child_indices) {
		auto keys_index = variant.GetKeysIndex(row, nested_data.children_idx + child_idx);
		auto &key = variant.GetKey(row, keys_index);
		sorted_fields.emplace(key, child_idx);
	}

	state.blob_size += VarintEncode(sorted_fields.size(), state.GetDestination());
	D_ASSERT(!sorted_fields.empty());

	uint32_t children_idx = state.children_size;
	uint32_t keys_idx = state.keys_size;
	state.blob_size += VarintEncode(children_idx, state.GetDestination());
	state.children_size += sorted_fields.size();
	state.keys_size += sorted_fields.size();

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

static vector<uint32_t> UnshreddedObjectChildren(UnifiedVariantVectorData &variant, uint32_t row, uint32_t value_index,
                                                 DuckDBVariantShreddingState &shredding_state) {
	auto nested_data = VariantUtils::DecodeNestedData(variant, row, value_index);

	auto shredded_fields = shredding_state.ObjectFields();
	vector<uint32_t> unshredded_children;
	unshredded_children.reserve(nested_data.child_count);
	for (uint32_t i = 0; i < nested_data.child_count; i++) {
		auto keys_index = variant.GetKeysIndex(row, nested_data.children_idx + i);
		auto &key = variant.GetKey(row, keys_index);
		if (shredded_fields.count(key)) {
			continue;
		}
		unshredded_children.emplace_back(i);
	}
	return unshredded_children;
}

//! ~~Write the unshredded values~~, also receiving the 'untyped_value_index' Vector to populate
//! Marking the rows that are shredded in the shredding state
void DuckDBVariantShredding::AnalyzeVariantValues(UnifiedVariantVectorData &variant, Vector &value,
                                                  optional_ptr<const SelectionVector> sel,
                                                  optional_ptr<const SelectionVector> value_index_sel,
                                                  optional_ptr<const SelectionVector> result_sel,
                                                  DuckDBVariantShreddingState &shredding_state, idx_t count) {
	auto &validity = FlatVector::Validity(value);
	auto untyped_data = FlatVector::GetData<uint32_t>(value);

	for (uint32_t i = 0; i < static_cast<uint32_t>(count); i++) {
		uint32_t value_index = 0;
		if (value_index_sel) {
			value_index = static_cast<uint32_t>(value_index_sel->get_index(i));
		}

		uint32_t row = i;
		if (sel) {
			row = static_cast<uint32_t>(sel->get_index(i));
		}

		idx_t result_index = i;
		if (result_sel) {
			result_index = result_sel->get_index(i);
		}

		if (variant.RowIsValid(row) && shredding_state.ValueIsShredded(variant, row, value_index)) {
			shredding_state.SetShredded(row, value_index, result_index);
			if (shredding_state.type.id() != LogicalTypeId::STRUCT) {
				//! Value is shredded, directly write a NULL to the 'value' if the type is not an OBJECT
				validity.SetInvalid(result_index);
				continue;
			}

			//! When the type is OBJECT, all excess fields would still need to be written to the 'value'
			auto unshredded_children = UnshreddedObjectChildren(variant, row, value_index, shredding_state);
			if (unshredded_children.empty()) {
				//! Fully shredded object
				validity.SetInvalid(result_index);
			} else {
				//! Deal with partially shredded objects
				unshredded_values[row].emplace_back(value_index, untyped_data[result_index],
				                                    std::move(unshredded_children));
			}
			continue;
		}

		//! Deal with unshredded values
		if (!variant.RowIsValid(row) || variant.GetTypeId(row, value_index) == VariantLogicalType::VARIANT_NULL) {
			//! 0 is reserved for NULL
			untyped_data[result_index] = 0;
		} else {
			unshredded_values[row].emplace_back(value_index, untyped_data[result_index]);
		}
	}
}

//! Receive a 'shredded' result Vector, consisting of the 'untyped_value_index' and the 'typed_value' Vector
void DuckDBVariantShredding::WriteVariantValues(UnifiedVariantVectorData &variant, Vector &result,
                                                optional_ptr<const SelectionVector> sel,
                                                optional_ptr<const SelectionVector> value_index_sel,
                                                optional_ptr<const SelectionVector> result_sel, idx_t count) {
	auto &result_type = result.GetType();
	D_ASSERT(result_type.id() == LogicalTypeId::STRUCT);
	auto &child_types = StructType::GetChildTypes(result_type);
	auto &child_vectors = StructVector::GetEntries(result);
	D_ASSERT(child_types.size() == child_vectors.size());

	auto &untyped_value_index = *child_vectors[0];
	auto &typed_value = *child_vectors[1];

	DuckDBVariantShreddingState shredding_state(typed_value.GetType(), count);
	AnalyzeVariantValues(variant, untyped_value_index, sel, value_index_sel, result_sel, shredding_state, count);

	SelectionVector null_values;
	if (shredding_state.count) {
		WriteTypedValues(variant, typed_value, shredding_state.shredded_sel, shredding_state.values_index_sel,
		                 shredding_state.result_sel, shredding_state.count);
		//! Set the rows that aren't shredded to NULL
		idx_t sel_idx = 0;
		for (idx_t i = 0; i < count; i++) {
			auto original_index = result_sel ? result_sel->get_index(i) : i;
			if (sel_idx < shredding_state.count && shredding_state.result_sel[sel_idx] == original_index) {
				sel_idx++;
				continue;
			}
			FlatVector::SetNull(typed_value, original_index, true);
		}
	} else {
		//! Set all rows of the typed_value to NULL, nothing is shredded on
		for (idx_t i = 0; i < count; i++) {
			FlatVector::SetNull(typed_value, result_sel ? result_sel->get_index(i) : i, true);
		}
	}
}

void VariantColumnData::ShredVariantData(Vector &input, Vector &output, idx_t count, const LogicalType &shredded_type) {
	RecursiveUnifiedVectorFormat recursive_format;
	Vector::RecursiveToUnifiedFormat(input, count, recursive_format);
	UnifiedVariantVectorData variant(recursive_format);

	auto &child_vectors = StructVector::GetEntries(output);

	//! First traverse the Variant to write the shredded values and collect the 'untyped_value_index'es
	DuckDBVariantShredding shredding(count);
	shredding.WriteVariantValues(variant, *child_vectors[1], nullptr, nullptr, nullptr, count);

	//! Now we can write the unshredded values
	auto &unshredded = *child_vectors[0];
	auto original_keys_size = ListVector::GetListSize(VariantVector::GetKeys(input));
	auto original_children_size = ListVector::GetListSize(VariantVector::GetChildren(input));
	auto original_values_size = ListVector::GetListSize(VariantVector::GetValues(input));

	auto &keys = VariantVector::GetKeys(unshredded);
	auto &children = VariantVector::GetChildren(unshredded);
	auto &values = VariantVector::GetValues(unshredded);
	auto &data = VariantVector::GetData(unshredded);

	ListVector::Reserve(keys, original_keys_size);
	ListVector::SetListSize(keys, 0);
	ListVector::Reserve(children, original_children_size);
	ListVector::SetListSize(children, 0);
	ListVector::Reserve(values, original_values_size);
	ListVector::SetListSize(values, 0);

	auto &keys_entry = ListVector::GetEntry(keys);
	OrderedOwningStringMap<uint32_t> dictionary(StringVector::GetStringBuffer(keys_entry).GetStringAllocator());
	SelectionVector keys_selvec;
	keys_selvec.Initialize(original_keys_size);

	VariantVectorData variant_data(unshredded);
	for (idx_t row = 0; row < count; row++) {
		auto &unshredded_values = shredding.unshredded_values[row];

		if (unshredded_values.empty()) {
			FlatVector::SetNull(unshredded, row, true);
			continue;
		}

		//! Allocate for the new data, use the same size as source
		auto &blob_data = variant_data.blob_data[row];
		auto original_data = variant.GetData(row);
		blob_data = StringVector::EmptyString(data, original_data.GetSize());

		auto &keys_list_entry = variant_data.keys_data[row];
		keys_list_entry.offset = ListVector::GetListSize(keys);

		auto &children_list_entry = variant_data.children_data[row];
		children_list_entry.offset = ListVector::GetListSize(children);

		auto &values_list_entry = variant_data.values_data[row];
		values_list_entry.offset = ListVector::GetListSize(values);

		VariantNormalizerState normalizer_state(row, variant_data, dictionary, keys_selvec);
		for (idx_t i = 0; i < unshredded_values.size(); i++) {
			auto &unshredded_value = unshredded_values[i];
			auto value_index = unshredded_value.source_value_index;

			unshredded_value.target_value_index = normalizer_state.values_size + 1;
			if (!unshredded_value.unshredded_children.empty()) {
				D_ASSERT(variant.GetTypeId(row, value_index) == VariantLogicalType::OBJECT);
				auto nested_data = VariantUtils::DecodeNestedData(variant, row, value_index);

				normalizer_state.type_ids[normalizer_state.values_size] =
				    static_cast<uint8_t>(VariantLogicalType::OBJECT);
				normalizer_state.byte_offsets[normalizer_state.values_size] = normalizer_state.blob_size;
				normalizer_state.values_size++;
				VisitObject(variant, row, nested_data, normalizer_state, unshredded_value.unshredded_children);
				continue;
			}
			VariantVisitor<VariantNormalizer>::Visit(variant, row, value_index, normalizer_state);
		}
		blob_data.SetSizeAndFinalize(normalizer_state.blob_size, original_data.GetSize());
		keys_list_entry.length = normalizer_state.keys_size;
		children_list_entry.length = normalizer_state.children_size;
		values_list_entry.length = normalizer_state.values_size;

		ListVector::SetListSize(keys, ListVector::GetListSize(keys) + normalizer_state.keys_size);
		ListVector::SetListSize(children, ListVector::GetListSize(children) + normalizer_state.children_size);
		ListVector::SetListSize(values, ListVector::GetListSize(values) + normalizer_state.values_size);
	}

	VariantUtils::FinalizeVariantKeys(unshredded, dictionary, keys_selvec, ListVector::GetListSize(keys));
	keys_entry.Slice(keys_selvec, ListVector::GetListSize(keys));

	if (input.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		unshredded.SetVectorType(VectorType::CONSTANT_VECTOR);
	}

#ifdef DEBUG
	Vector roundtrip_result(LogicalType::VARIANT(), count);
	VariantColumnData::UnshredVariantData(output, roundtrip_result, count);

	for (idx_t i = 0; i < count; i++) {
		auto input_val = input.GetValue(i);
		auto roundtripped_val = roundtrip_result.GetValue(i);
		if (!ValueOperations::NotDistinctFrom(input_val, roundtripped_val)) {
			throw InternalException("Shredding roundtrip verification failed for row: %d, expected: %s, actual: %s", i,
			                        input_val.ToString(), roundtripped_val.ToString());
		}
	}

#endif
}

} // namespace duckdb
