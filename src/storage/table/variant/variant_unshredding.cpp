#include "duckdb/storage/table/variant_column_data.hpp"
#include "duckdb/common/types/variant.hpp"
#include "duckdb/function/cast/variant/to_variant_fwd.hpp"
#include "duckdb/common/types/variant_value.hpp"
#include "duckdb/common/types/variant_visitor.hpp"
#include "duckdb/function/variant/variant_value_convert.hpp"

namespace duckdb {

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
			auto val = UnshreddedVariantValue(input, row, child_values_index);

			res.AddItem(std::move(val));
		}
		return res;
	}
	auto val = VariantVisitor<ValueConverter>::Visit(input, row, values_index);
	return VariantValue(std::move(val));
}

static vector<VariantValue> Unshred(UnifiedVariantVectorData &variant, Vector &shredded, idx_t count);

static vector<VariantValue> UnshredTypedLeaf(Vector &typed_value, idx_t count) {
	vector<VariantValue> res(count);
	UnifiedVectorFormat vector_format;
	typed_value.ToUnifiedFormat(count, vector_format);
	auto &typed_value_validity = vector_format.validity;

	for (idx_t i = 0; i < count; i++) {
		if (!typed_value_validity.RowIsValid(i)) {
			continue;
		}
		res[i] = VariantValue(typed_value.GetValue(i));
	}
	return res;
}

static vector<VariantValue> UnshredTypedObject(UnifiedVariantVectorData &variant, Vector &typed_value, idx_t count) {
	vector<VariantValue> res(count);

	auto &child_types = StructType::GetChildTypes(typed_value.GetType());
	auto &child_entries = StructVector::GetEntries(typed_value);

	D_ASSERT(child_types.size() == child_entries.size());

	//! First unshred all children
	vector<vector<VariantValue>> child_values;
	for (idx_t child_idx = 0; child_idx < child_entries.size(); child_idx++) {
		auto &child_entry = child_entries[child_idx];
		child_values[child_idx] = Unshred(variant, *child_entry, count);
	}

	//! Then compose the OBJECT value by combining all the children
	UnifiedVectorFormat vector_format;
	typed_value.ToUnifiedFormat(count, vector_format);
	auto &typed_value_validity = vector_format.validity;
	for (idx_t child_idx = 0; child_idx < child_entries.size(); child_idx++) {
		auto &child_name = child_types[child_idx].first;
		auto &values = child_values[child_idx];

		for (idx_t i = 0; i < count; i++) {
			if (typed_value_validity.RowIsValid(i)) {
				continue;
			}
			if (values[i].IsMissing()) {
				continue;
			}
			if (res[i].IsMissing()) {
				res[i] = VariantValue(VariantValueType::OBJECT);
			}
			auto &obj_value = res[i];
			obj_value.AddChild(child_name, std::move(values[i]));
		}
	}
	return res;
}

static vector<VariantValue> UnshredTypedArray(UnifiedVariantVectorData &variant, Vector &typed_value, idx_t count) {
	auto child_size = ListVector::GetListSize(typed_value);
	auto &child_vector = ListVector::GetEntry(typed_value);
	auto child_values = Unshred(variant, child_vector, child_size);

	D_ASSERT(typed_value.GetType().id() == LogicalTypeId::LIST);
	auto list_data = FlatVector::GetData<list_entry_t>(typed_value);

	UnifiedVectorFormat vector_format;
	typed_value.ToUnifiedFormat(count, vector_format);
	auto &typed_value_validity = vector_format.validity;

	vector<VariantValue> res(count);
	for (idx_t i = 0; i < count; i++) {
		if (!typed_value_validity.RowIsValid(i)) {
			continue;
		}
		auto &list_entry = list_data[i];

		auto &list_val = res[i];
		list_val = VariantValue(VariantValueType::ARRAY);
		list_val.array_items.reserve(list_entry.length);
		list_val.array_items.insert(
		    list_val.array_items.end(), std::make_move_iterator(child_values.begin() + list_entry.offset),
		    std::make_move_iterator(child_values.begin() + list_entry.offset + list_entry.length));
	}
	return res;
}

static vector<VariantValue> UnshredTypedValue(UnifiedVariantVectorData &variant, Vector &typed_value, idx_t count) {
	auto &type = typed_value.GetType();
	if (type.id() == LogicalTypeId::STRUCT) {
		return UnshredTypedObject(variant, typed_value, count);
	} else if (type.id() == LogicalTypeId::LIST) {
		return UnshredTypedArray(variant, typed_value, count);
	} else {
		D_ASSERT(!type.IsNested());
		return UnshredTypedLeaf(typed_value, count);
	}
}

static vector<VariantValue> Unshred(UnifiedVariantVectorData &variant, Vector &shredded, idx_t count) {
	D_ASSERT(shredded.GetType().id() == LogicalTypeId::STRUCT);
	auto &child_entries = StructVector::GetEntries(shredded);
	D_ASSERT(child_entries.size() == 2);

	auto &untyped_value_index = *child_entries[0];
	auto &typed_value = *child_entries[1];

	UnifiedVectorFormat untyped_format;
	untyped_value_index.ToUnifiedFormat(count, untyped_format);
	auto untyped_index_data = untyped_format.GetData<uint32_t>(untyped_format);
	auto &untyped_index_validity = untyped_format.validity;

	auto res = UnshredTypedValue(variant, typed_value, count);
	for (idx_t i = 0; i < count; i++) {
		if (!untyped_index_validity.RowIsValid(untyped_format.sel->get_index(i))) {
			continue;
		}
		auto value_index = untyped_index_data[untyped_format.sel->get_index(i)];
		if (res[i].IsMissing()) {
			//! Unshredded, has no shredded value
			res[i] = UnshreddedVariantValue(variant, i, value_index);
		} else {
			//! Partial shredding, already has a shredded value that this has to be combined into
			D_ASSERT(res[i].value_type == VariantValueType::OBJECT);
			auto unshredded = UnshreddedVariantValue(variant, i, value_index);
			D_ASSERT(unshredded.value_type == VariantValueType::OBJECT);
			auto &object_children = unshredded.object_children;
			for (auto &entry : object_children) {
				res[i].AddChild(entry.first, std::move(entry.second));
			}
		}
	}
	return res;
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

	auto variant_values = Unshred(variant, shredded, count);
	VariantValue::ToVARIANT(variant_values, output);
}

} // namespace duckdb
