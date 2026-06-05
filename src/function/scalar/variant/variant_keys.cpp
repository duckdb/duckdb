#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/types/variant.hpp"
#include "duckdb/function/scalar/variant_functions.hpp"
#include "duckdb/function/scalar/variant_utils.hpp"

namespace duckdb {

static void WriteKeyIdList(const UnifiedVariantVectorData &variant, Vector &key_ids,
                           array_ptr<VariantNestedData> nested_data, const ValidityMask &object_validity,
                           const idx_t count) {
	auto writer = FlatVector::Writer<VectorListType<idx_t>>(key_ids, count);
	const auto &list_validity = FlatVector::Validity(key_ids);

	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		if (!list_validity.RowIsValid(row_idx)) {
			writer.WriteNull();
			continue;
		}
		if (!object_validity.RowIsValid(row_idx)) {
			writer.WriteList(0);
			continue;
		}

		const auto &[child_count, children_idx] = nested_data[row_idx];
		auto row_writer = writer.WriteList(child_count);

		idx_t child_idx = 0;
		for (auto &key_id_writer : row_writer) {
			const auto key_id = variant.GetKeysIndex(row_idx, children_idx + child_idx);
			key_id_writer.WriteValue(key_id);
			child_idx++;
		}
	}
}

// TODO: Currently collection will always happen on the unshredded variant, introduce a fast path for shredded variants.
static Vector CollectVariantKeys(const UnifiedVariantVectorData &variant,
                                 const vector<VariantPathComponent> &components, const idx_t count) {
	// By row found keys at the requested path in the VARIANT
	Vector key_ids(LogicalType::LIST(LogicalType::UBIGINT), count);
	VariantPathSelection path_selection(count);

	const auto owned_nested_data = make_unsafe_uniq_array_uninitialized<VariantNestedData>(count);
	const array_ptr nested_data(owned_nested_data.get(), count);

	auto &list_validity = FlatVector::ValidityMutable(key_ids);
	VariantUtils::TraversePath(variant, components, count, nested_data, list_validity, path_selection);

	// For the final collection of nested_data we use an auxiliary validity vector so we can distinguish "path missing"
	// from "path exists but not an object" (producing NULL and [] as output respectively).
	ValidityMask object_validity(count);
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		if (!list_validity.RowIsValid(row_idx)) {
			object_validity.SetInvalid(row_idx);
		}
	}

	const auto &final_indices = path_selection.Input(components.size());
	(void)VariantUtils::CollectNestedData(variant, VariantLogicalType::OBJECT, final_indices, count, optional_idx(), 0,
	                                      nested_data, object_validity);

	WriteKeyIdList(variant, key_ids, nested_data, object_validity, count);

	return key_ids;
}

static void UnaryVariantKeys(const Vector &variant_vec, const vector<VariantPathComponent> &components, Vector &result,
                             const idx_t count) {
	RecursiveUnifiedVectorFormat source_format;
	Vector::RecursiveToUnifiedFormat(variant_vec, source_format);
	const UnifiedVariantVectorData variant(source_format);

	auto key_ids = CollectVariantKeys(variant, components, count);
	const auto &list_validity = FlatVector::Validity(key_ids);
	const auto list_entries = FlatVector::GetData<const list_entry_t>(key_ids);
	const auto &child = ListVector::GetChild(key_ids);
	const auto key_ids_data = FlatVector::GetData<const idx_t>(child);

	result.Initialize(VectorDataInitialization::UNINITIALIZED, count);
	auto result_writer = FlatVector::Writer<VectorListType<string_t>>(result, count);

	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		if (!list_validity.RowIsValid(row_idx)) {
			result_writer.WriteNull();
			continue;
		}

		auto &entry = list_entries[row_idx];
		auto row_writer = result_writer.WriteList(entry.length);

		idx_t key_idx = 0;
		for (auto &key_writer : row_writer) {
			const auto key_id = key_ids_data[entry.offset + key_idx++];
			key_writer.WriteValue(variant.GetKey(row_idx, key_id));
		}
	}
}

static void ManyVariantKeys(const Vector &variant_vec, const vector<vector<VariantPathComponent>> &paths,
                            Vector &result, const idx_t count) {
	vector<Vector> keys_by_path;
	keys_by_path.reserve(paths.size());

	RecursiveUnifiedVectorFormat source_format;
	Vector::RecursiveToUnifiedFormat(variant_vec, source_format);
	const UnifiedVariantVectorData variant(source_format);

	for (const auto &path : paths) {
		keys_by_path.push_back(CollectVariantKeys(variant, path, count));
	}

	result.Initialize(VectorDataInitialization::UNINITIALIZED, count);
	auto result_writer = FlatVector::Writer<VectorListType<VectorListType<string_t>>>(result, count);

	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		auto row_writer = result_writer.WriteList(paths.size());
		idx_t path_idx = 0;
		for (auto &path_keys_writer : row_writer) {
			const auto &key_ids = keys_by_path[path_idx];
			const auto &list_validity = FlatVector::Validity(key_ids);
			const auto list_entries = FlatVector::GetData<const list_entry_t>(key_ids);
			const auto &child = ListVector::GetChild(key_ids);
			const auto key_ids_data = FlatVector::GetData<const idx_t>(child);

			if (!list_validity.RowIsValid(row_idx)) {
				path_keys_writer.WriteNull();
				path_idx++;
				continue;
			}

			auto &entry = list_entries[row_idx];
			auto keys_writer = path_keys_writer.WriteList(entry.length);

			idx_t key_idx = 0;
			for (auto &key_writer : keys_writer) {
				const auto key_id = key_ids_data[entry.offset + key_idx++];
				key_writer.WriteValue(variant.GetKey(row_idx, key_id));
			}

			path_idx++;
		}
	}
}

static void VariantKeysFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	VariantUtils::ExecutePathFunction(input, state, result, UnaryVariantKeys, ManyVariantKeys);
}

static void AddFunctionsWithParameterType(ScalarFunctionSet &fun_set, const LogicalType &input_type) {
	ScalarFunction variant_keys("variant_keys", {}, LogicalType::LIST(LogicalType::VARCHAR), VariantKeysFunction,
	                            VariantBindUtils::VariantPathBind, nullptr);

	variant_keys.GetSignature().AddParameter(input_type);
	fun_set.AddFunction(variant_keys);

	variant_keys.GetSignature().AddParameter(LogicalType::VARCHAR);
	fun_set.AddFunction(variant_keys);

	variant_keys.GetSignature().GetParameter(1).SetType(LogicalType::LIST(LogicalType::VARCHAR));
	variant_keys.SetReturnType(LogicalType::LIST(LogicalType::LIST(LogicalType::VARCHAR)));
	fun_set.AddFunction(variant_keys);
}

ScalarFunctionSet VariantKeysFun::GetFunctions() {
	ScalarFunctionSet fun_set;

	AddFunctionsWithParameterType(fun_set, LogicalType::VARIANT());

	return fun_set;
}

} // namespace duckdb
