#include "duckdb/storage/table/variant_column_data.hpp"
#include "duckdb/common/types/variant.hpp"
#include "duckdb/common/types/variant_visitor.hpp"
#include "duckdb/function/variant/variant_shredding.hpp"
#include "duckdb/function/variant/variant_normalize.hpp"
#include "duckdb/common/serializer/varint.hpp"

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
	case LogicalTypeId::TIMESTAMP_TZ:
		return {VariantLogicalType::TIMESTAMP_MICROS_TZ};
	case LogicalTypeId::TIMESTAMP:
		return {VariantLogicalType::TIMESTAMP_MICROS};
	case LogicalTypeId::TIMESTAMP_NS:
		return {VariantLogicalType::TIMESTAMP_NANOS};
	case LogicalTypeId::BLOB:
		return {VariantLogicalType::BLOB};
	case LogicalTypeId::VARCHAR:
		return {VariantLogicalType::VARCHAR};
	case LogicalTypeId::UUID:
		return {VariantLogicalType::UUID};
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

struct DuckDBVariantShredding : public VariantShredding {
public:
	DuckDBVariantShredding(VariantVectorData &source, OrderedOwningStringMap<uint32_t> &dictionary,
	                       SelectionVector &keys_selvec)
	    : VariantShredding(), variant_source(source), dictionary(dictionary), keys_selvec(keys_selvec) {
	}
	~DuckDBVariantShredding() override = default;

public:
	void WriteVariantValues(UnifiedVariantVectorData &variant, Vector &result, optional_ptr<const SelectionVector> sel,
	                        optional_ptr<const SelectionVector> value_index_sel,
	                        optional_ptr<const SelectionVector> result_sel, idx_t count) override;
	void CreateValues(UnifiedVariantVectorData &variant, Vector &value, optional_ptr<const SelectionVector> sel,
	                  optional_ptr<const SelectionVector> value_index_sel,
	                  optional_ptr<const SelectionVector> result_sel,
	                  optional_ptr<DuckDBVariantShreddingState> shredding_state, idx_t count);

private:
	VariantVectorData &variant_source;
	OrderedOwningStringMap<uint32_t> &dictionary;
	SelectionVector &keys_selvec;
};

} // namespace

static void VisitObject(const UnifiedVariantVectorData &variant, idx_t row, const VariantNestedData &nested_data,
                        VariantNormalizerState &state, DuckDBVariantShreddingState &shredding_state) {
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

void DuckDBVariantShredding::CreateValues(UnifiedVariantVectorData &variant, Vector &value,
                                          optional_ptr<const SelectionVector> sel,
                                          optional_ptr<const SelectionVector> value_index_sel,
                                          optional_ptr<const SelectionVector> result_sel,
                                          optional_ptr<DuckDBVariantShreddingState> shredding_state, idx_t count) {
	auto &validity = FlatVector::Validity(value);
	auto value_data = FlatVector::GetData<string_t>(value);

	for (idx_t i = 0; i < count; i++) {
		idx_t value_index = 0;
		if (value_index_sel) {
			value_index = value_index_sel->get_index(i);
		}

		idx_t row = i;
		if (sel) {
			row = sel->get_index(i);
		}

		idx_t result_index = i;
		if (result_sel) {
			result_index = result_sel->get_index(i);
		}

		VariantNormalizerState normalizer_state(result_index, variant_source, dictionary, keys_selvec);

		bool is_shredded = false;
		if (variant.RowIsValid(row) && shredding_state && shredding_state->ValueIsShredded(variant, row, value_index)) {
			shredding_state->SetShredded(row, value_index, result_index);
			is_shredded = true;
			if (shredding_state->type.id() != LogicalTypeId::STRUCT) {
				//! Value is shredded, directly write a NULL to the 'value' if the type is not an OBJECT
				//! When the type is OBJECT, all excess fields would still need to be written to the 'value'
				validity.SetInvalid(result_index);
				continue;
			}
			auto nested_data = VariantUtils::DecodeNestedData(variant, row, value_index);
			VisitObject(variant, row, nested_data, normalizer_state, *shredding_state);
		} else {
			VariantVisitor<VariantNormalizer>::Visit(variant, row, value_index, normalizer_state);
		}
	}
}

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
	CreateValues(variant, untyped_value_index, sel, value_index_sel, result_sel, &shredding_state, count);

	SelectionVector null_values;
	if (shredding_state.count) {
		WriteTypedValues(variant, typed_value, shredding_state.shredded_sel, shredding_state.values_index_sel,
		                 shredding_state.result_sel, shredding_state.count);
		//! 'shredding_state.result_sel' will always be a subset of 'result_sel', set the rows not in the subset to
		//! NULL
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

static void PrepareUnshreddedVector(UnifiedVariantVectorData &variant, Vector &input, Vector &unshredded, idx_t count) {
	//! Take the original sizes of the lists, the result will be similar size, never bigger
}

void VariantColumnData::ShredVariantData(Vector &input, Vector &output, idx_t count, const LogicalType &shredded_type) {
	RecursiveUnifiedVectorFormat recursive_format;
	Vector::RecursiveToUnifiedFormat(input, count, recursive_format);
	UnifiedVariantVectorData variant(recursive_format);

	auto &child_vectors = StructVector::GetEntries(output);

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

	VariantVectorData variant_data(unshredded);
	for (idx_t i = 0; i < count; i++) {
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
	}

	auto &keys_entry = ListVector::GetEntry(keys);
	OrderedOwningStringMap<uint32_t> dictionary(StringVector::GetStringBuffer(keys_entry).GetStringAllocator());
	SelectionVector keys_selvec;
	keys_selvec.Initialize(original_keys_size);

	DuckDBVariantShredding shredding(variant_data, dictionary, keys_selvec);
	shredding.WriteVariantValues(variant, *child_vectors[1], nullptr, nullptr, nullptr, count);
}

} // namespace duckdb
