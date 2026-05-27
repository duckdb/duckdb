#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/types/variant.hpp"
#include "duckdb/function/scalar/variant_functions.hpp"
#include "duckdb/function/scalar/variant_utils.hpp"

namespace duckdb {

// TODO: Currently collection will always happen on the unshredded variant, introduce a fast path for shredded variants.
static ValidityMask CollectVariantExistence(const UnifiedVariantVectorData &variant,
                                            const vector<VariantPathComponent> &components, const idx_t count) {
	ValidityMask path_validity(count);
	VariantPathSelection path_selection(count);

	const auto owned_nested_data = make_unsafe_uniq_array_uninitialized<VariantNestedData>(count);
	const array_ptr nested_data(owned_nested_data.get(), count);

	VariantUtils::TraversePath(variant, components, count, nested_data, path_validity, path_selection);

	return path_validity;
}

static void UnaryVariantExists(const Vector &variant_vec, const vector<VariantPathComponent> &components,
                               Vector &result, const idx_t count) {
	RecursiveUnifiedVectorFormat source_format;
	Vector::RecursiveToUnifiedFormat(variant_vec, source_format);
	const UnifiedVariantVectorData variant(source_format);

	const auto &path_validity = CollectVariantExistence(variant, components, count);

	result.Initialize(VectorDataInitialization::UNINITIALIZED, count);
	auto row_writer = FlatVector::Writer<bool>(result, count);

	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		if (!variant.RowIsValid(row_idx)) {
			row_writer.WriteNull();
			continue;
		}

		if (path_validity.RowIsValid(row_idx)) {
			row_writer.WriteValue(true);
		} else {
			row_writer.WriteValue(false);
		}
	}
}

static void ManyVariantExists(const Vector &variant_vec, const vector<vector<VariantPathComponent>> &paths,
                              Vector &result, const idx_t count) {
	vector<ValidityMask> existence_by_path;
	existence_by_path.reserve(paths.size());

	RecursiveUnifiedVectorFormat source_format;
	Vector::RecursiveToUnifiedFormat(variant_vec, source_format);
	const UnifiedVariantVectorData variant(source_format);

	for (const auto &path : paths) {
		existence_by_path.push_back(CollectVariantExistence(variant, path, count));
	}

	result.Initialize(VectorDataInitialization::UNINITIALIZED, count);
	auto result_writer = FlatVector::Writer<VectorListType<bool>>(result, count);

	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		if (!variant.RowIsValid(row_idx)) {
			result_writer.WriteNull();
			continue;
		}

		auto row_writer = result_writer.WriteList(paths.size());
		idx_t path_idx = 0;
		for (auto &path_existence_writer : row_writer) {
			if (existence_by_path[path_idx].RowIsValid(row_idx)) {
				path_existence_writer.WriteValue(true);
			} else {
				path_existence_writer.WriteValue(false);
			}

			path_idx++;
		}
	}
}

static void VariantExistsFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	VariantUtils::ExecutePathFunction(input, state, result, UnaryVariantExists, ManyVariantExists);
}

ScalarFunctionSet VariantExistsFun::GetFunctions() {
	ScalarFunctionSet fun_set;

	ScalarFunction variant_exists("variant_exists", {LogicalType::VARIANT(), LogicalType::VARCHAR},
	                              LogicalType::BOOLEAN, VariantExistsFunction, VariantBindUtils::VariantPathBind,
	                              nullptr);
	fun_set.AddFunction(variant_exists);

	variant_exists.GetSignature().GetParameter(1).SetType(LogicalType::LIST(LogicalType::VARCHAR));
	variant_exists.SetReturnType(LogicalType::LIST(LogicalType::BOOLEAN));
	fun_set.AddFunction(variant_exists);

	return fun_set;
}

} // namespace duckdb
