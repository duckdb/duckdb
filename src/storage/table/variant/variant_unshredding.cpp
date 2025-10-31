#include "duckdb/storage/table/variant_column_data.hpp"
#include "duckdb/common/types/variant.hpp"
#include "duckdb/function/cast/variant/to_variant_fwd.hpp"
#include "duckdb/common/types/variant_value.hpp"
#include "duckdb/common/types/variant_visitor.hpp"
#include "duckdb/function/variant/variant_value_convert.hpp"

namespace duckdb {

static vector<VariantValue> Unshred(UnifiedVariantVectorData &variant, Vector &shredded, idx_t count) {
	vector<VariantValue> res;

	D_ASSERT(shredded.GetType().id() == LogicalTypeId::STRUCT);
	auto &child_entries = StructVector::GetEntries(shredded);
	D_ASSERT(child_entries.size() == 2);

	auto &untyped_value_index = *child_entries[0];
	auto &typed_value = *child_entries[1];

	auto untyped_index_data = FlatVector::GetData<uint32_t>(untyped_value_index);
	auto &untyped_index_validity = FlatVector::Validity(untyped_value_index);

	auto &typed_value_validity = FlatVector::Validity(typed_value);
	for (idx_t i = 0; i < count; i++) {
		if (typed_value_validity.RowIsValid(i)) {
			//! Shredded, check if it's partially shredded
			if (untyped_index_validity.RowIsValid(i)) {
			}
		} else if (untyped_index_validity.RowIsValid(i)) {
		}
	}
}

static VariantValue UnshreddedVariantValue(UnifiedVariantVectorData &input, uint32_t row, uint32_t values_index) {
	if (input.RowIsValid(row)) {
		return VariantValue(Value(LogicalTypeId::SQLNULL));
	}
	auto type_id = input.GetTypeId(row, values_index);
	if (type_id == VariantLogicalType::VARIANT_NULL) {
		return VariantValue(Value(LogicalTypeId::SQLNULL));
	}

	if (type_id == VariantLogicalType::OBJECT) {
		VariantValue res(VariantValueType::OBJECT);

		auto object_data = VariantUtils::DecodeNestedData(input, row, values_index);
		for (idx_t i = 0; i < object_data.child_count; i++) {
			auto child_values_index = input.GetValuesIndex(row, object_data.children_idx + i);
			auto val = UnshreddedVariantValue(input, row, child_values_index);

			auto keys_index = input.GetKeysIndex(row, object_data.children_idx + i);
			auto &key = input.GetKey(row, keys_index);

			res.AddChild(key.GetString(), std::move(val));
		}
		return res;
	}
	if (type_id == VariantLogicalType::ARRAY) {
		VariantValue res(VariantValueType::ARRAY);

		auto array_data = VariantUtils::DecodeNestedData(input, row, values_index);
		for (idx_t i = 0; i < array_data.child_count; i++) {
			auto child_values_index = input.GetValuesIndex(row, array_data.children_idx + i);
			auto val = UnshreddedVariantValue(input, row, values_index);

			res.AddItem(std::move(val));
		}
		return res;
	}
	auto val = VariantVisitor<ValueConverter>::Visit(input, row, values_index);
	return VariantValue(std::move(val));
}

void VariantColumnData::UnshredVariantData(Vector &input, Vector &output, idx_t count) {
	D_ASSERT(input.GetType().id() == LogicalTypeId::STRUCT);
	auto &child_vectors = StructVector::GetEntries(input);
	D_ASSERT(child_vectors.size() == 2);

	auto &unshredded = *child_vectors[0];
	auto &shredded = *child_vectors[1];

	RecursiveUnifiedVectorFormat recursive_format;
	Vector::RecursiveToUnifiedFormat(unshredded, count, recursive_format);
	UnifiedVariantVectorData variant(recursive_format);

	//! First convert all the unshredded values at the root
	vector<VariantValue> res(count);
	for (idx_t i = 0; i < count; i++) {
		res[i] = UnshreddedVariantValue(variant, i, 0);
	}

	auto shredded_values = Unshred(variant, shredded, count);
}

} // namespace duckdb
