#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/shredded_vector.hpp"
#include "duckdb/common/vector/variant_vector.hpp"
#include "duckdb/common/types/variant.hpp"
#include "duckdb/function/scalar/variant_functions.hpp"
#include "duckdb/function/scalar/variant_utils.hpp"
#include "duckdb/function/cast/variant/to_variant.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/execution/expression_executor.hpp"

namespace duckdb {

VariantKeysBindData::VariantKeysBindData() : FunctionData() {
}
VariantKeysBindData::VariantKeysBindData(const string &input_path) : FunctionData() {
	if (input_path.empty()) {
		paths.emplace_back();
	} else {
		paths.push_back({VariantPathComponent(input_path)});
	}
}
VariantKeysBindData::VariantKeysBindData(const vector<string> &input_paths) : FunctionData() {
	for (auto &path : input_paths) {
		if (path.empty()) {
			paths.emplace_back();
		} else {
			paths.push_back({VariantPathComponent(path)});
		}
	}
}
VariantKeysBindData::VariantKeysBindData(uint32_t index) : FunctionData() {
	if (index == 0) {
		throw BinderException("Extracting index 0 from VARIANT(ARRAY) is invalid, indexes are 1-based");
	}
	paths = {{VariantPathComponent(index - 1)}};
}

unique_ptr<FunctionData> VariantKeysBindData::Copy() const {
	return make_uniq<VariantKeysBindData>(*this);
}

static bool VariantPathComponentEquals(const VariantPathComponent &a, const VariantPathComponent &b) {
	if (a.lookup_mode != b.lookup_mode) {
		return false;
	}
	if (a.lookup_mode == VariantChildLookupMode::BY_INDEX && a.index != b.index) {
		return false;
	}
	if (a.lookup_mode == VariantChildLookupMode::BY_KEY && a.key != b.key) {
		return false;
	}
	return true;
}

bool VariantKeysBindData::Equals(const FunctionData &other) const {
	auto &bind_data = other.Cast<VariantKeysBindData>();
	if (paths.size() != bind_data.paths.size()) {
		return false;
	}

	for (idx_t i = 0; i < paths.size(); i++) {
		if (paths[i].size() != bind_data.paths[i].size()) {
			return false;
		}
		for (idx_t j = 0; j < paths[i].size(); j++) {
			if (!VariantPathComponentEquals(paths[i][j], bind_data.paths[i][j])) {
				return false;
			}
		}
	}
	return true;
}

static vector<vector<string>> CollectVariantKeys(const UnifiedVariantVectorData &variant, const vector<VariantPathComponent> &components,
                            idx_t count) {
	vector<vector<string>> rows;
	rows.resize(count);

	auto &allocator = Allocator::DefaultAllocator();

	// Input and output buffers used during the object walk
	SelectionVector value_index_sel, new_value_index_sel;
	value_index_sel.Initialize(count);
	new_value_index_sel.Initialize(count);

	// We start at values[0] for every row.
	for (idx_t i = 0; i < count; i++) {
		value_index_sel[i] = 0;
	}

	// Construct a tracker for every row.
	auto owned_nested_data = allocator.Allocate(sizeof(VariantNestedData) * count);
	auto nested_data = reinterpret_cast<VariantNestedData *>(owned_nested_data.get());

	ValidityMask validity(count);
	for (idx_t i = 0; i < components.size(); i++) {
		auto &component = components[i];
		auto &input_indices = i % 2 == 0 ? value_index_sel : new_value_index_sel;
		auto &output_indices = i % 2 == 0 ? new_value_index_sel : value_index_sel;

		auto expected_type = component.lookup_mode == VariantChildLookupMode::BY_INDEX ? VariantLogicalType::ARRAY
		                                                                               : VariantLogicalType::OBJECT;

		(void)VariantUtils::CollectNestedData(variant, expected_type, input_indices, count, optional_idx(), 0,
		                                      nested_data, validity);
		//! Look up the value_index of the child we're extracting
		ValidityMask lookup_validity(count);
		VariantUtils::FindChildValues(variant, component, nullptr, output_indices, lookup_validity, nested_data,
		                              validity, count);

		for (idx_t j = 0; j < count; j++) {
			if (!validity.RowIsValid(j)) {
				continue;
			}
			if (lookup_validity.CanHaveNull() && !lookup_validity.RowIsValid(j)) {
				//! No child could be extracted, set to NULL
				validity.SetInvalid(j);
				continue;
			}
			//! Get the index into 'values'
			auto type_id = variant.GetTypeId(j, output_indices[j]);
			if (type_id == VariantLogicalType::VARIANT_NULL) {
				validity.SetInvalid(j);
			}
		}
	}

	auto &input_indices = components.size() % 2 == 0 ? value_index_sel : new_value_index_sel;
	(void)VariantUtils::CollectNestedData(variant, VariantLogicalType::OBJECT, input_indices, count, optional_idx(), 0,
	                                      nested_data, validity);

	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		if (!validity.RowIsValid(row_idx)) {
			continue;
		}

		const auto &[child_count, children_idx] = nested_data[row_idx];
		rows[row_idx].reserve(child_count);

		for (idx_t child_idx = 0; child_idx < child_count; child_idx++) {
			const auto key_id = variant.GetKeysIndex(row_idx, children_idx + child_idx);
			const auto &key = variant.GetKey(row_idx, key_id);

			rows[row_idx].push_back(key.GetString());
		}
	}

	return rows;
}

// TODO: Add fast path for shredded variant vector.
static void UnaryVariantKeys(const Vector &variant_vec, const vector<VariantPathComponent> &components, Vector &result,
                        idx_t count) {
	RecursiveUnifiedVectorFormat source_format;
	Vector::RecursiveToUnifiedFormat(variant_vec, source_format);
	const UnifiedVariantVectorData variant(source_format);

	const auto rows = CollectVariantKeys(variant, components, count);

	result.Initialize(VectorDataInitialization::UNINITIALIZED, count);
	auto result_writer = FlatVector::Writer<VectorListType<string_t>>(result, count);

	for (idx_t row_idx = 0; row_idx < rows.size(); row_idx++) {
		auto row_writer = result_writer.WriteList(rows[row_idx].size());
		idx_t child_idx = 0;
		for (auto &child_writer: row_writer) {
			child_writer.WriteValue(rows[row_idx][child_idx++]);
		}
	}
}

static void ManyVariantKeys(const Vector &variant_vec, const vector<vector<VariantPathComponent>> &paths, Vector &result,
						idx_t count) {
	vector<vector<vector<string>>> path_results;
	path_results.reserve(paths.size());

	RecursiveUnifiedVectorFormat source_format;
	Vector::RecursiveToUnifiedFormat(variant_vec, source_format);
	const UnifiedVariantVectorData variant(source_format);


	for (const auto &path : paths) {
		path_results.push_back(CollectVariantKeys(variant, path, count));
	}

	result.Initialize(VectorDataInitialization::UNINITIALIZED, count);
	auto result_writer = FlatVector::Writer<VectorListType<VectorListType<string_t>>>(result, count);

	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		auto row_writer = result_writer.WriteList(paths.size());
		idx_t path_idx = 0;
		for (auto &list_writer : row_writer) {
			auto path_writer = list_writer.WriteList(path_results[path_idx][row_idx].size());

			idx_t key_idx = 0;
			for (auto &key_writer: path_writer) {
				key_writer.WriteValue(path_results[path_idx][row_idx][key_idx++]);
			}

			path_idx++;
		}
	}
}

// TODO: Copied from GetConstantArgument, function should be moved to shared file.
// TODO: Also check what this function does.
static bool GetConstArgument(ClientContext &context, Expression &expr, Value &constant_arg) {
	if (!expr.IsFoldable()) {
		return false;
	}
	constant_arg = ExpressionExecutor::EvaluateScalar(context, expr);
	if (!constant_arg.IsNull()) {
		return true;
	}
	return false;
}

static unique_ptr<BaseStatistics> VariantKeysPropagateStats(ClientContext &context, FunctionStatisticsInput &input) {
	auto &child_stats = input.child_stats;
	auto &bind_data = input.bind_data;
	auto &variant_stats = child_stats[0];
	if (variant_stats.GetStatsType() != StatisticsType::VARIANT_STATS) {
		// TODO: It is probably sensible to not compute statistics here, but confirm. (we have transformed to another
		// type)
		return nullptr;
	}

	const auto &info = bind_data->Cast<VariantKeysBindData>();
	if (!VariantStats::IsShredded(variant_stats)) {
		return nullptr;
	}
	const auto &shredded_stats = VariantStats::GetShreddedStats(variant_stats);
	if (!VariantShreddedStats::IsFullyShredded(shredded_stats)) {
		// TODO: Why do we skip when VARIANT is not fully shredded? Can't we use some of the properties still?
		return nullptr;
	}
	// FIXME: This should not be static
	auto found_stats = VariantShreddedStats::FindChildStats(shredded_stats, info.paths[0][0]);
	if (!found_stats || !VariantShreddedStats::IsFullyShredded(*found_stats)) {
		// TODO: Why do we skip when VARIANT is not fully shredded? Can't we use some of the properties still?
		return nullptr;
	}

	return VariantStats::WrapExtractedFieldAsVariant(variant_stats, *found_stats);
}

static unique_ptr<FunctionData> VariantKeysBind(BindScalarFunctionInput &input) {
	auto &context = input.GetClientContext();
	auto &arguments = input.GetArguments();

	if (arguments.size() != 1 && arguments.size() != 2) {
		throw BinderException("'variant_keys' expects either one VARIANT column argument, or two VARIANT column and "
		                      "VARCHAR path arguments");
	}

	if (arguments.size() == 1) {
		return make_uniq<VariantKeysBindData>();
	}

	auto &path = *arguments[1];
	// TODO: Check for the element type somewhere.
	if (path.GetReturnType().id() != LogicalTypeId::VARCHAR && path.GetReturnType().id() != LogicalTypeId::LIST) {
		throw BinderException("'variant_keys' expects the second argument to be of type VARCHAR or VARCHAR[], not %s",
		                      path.GetReturnType().ToString());
	}

	Value constant_arg;
	if (!GetConstArgument(context, path, constant_arg)) {
		throw BinderException("'variant_keys' expects the second argument to be a constant expression");
	}

	if (constant_arg.type().id() == LogicalTypeId::VARCHAR) {
		return make_uniq<VariantKeysBindData>(constant_arg.GetValue<string>());
	} else if (constant_arg.type().id() == LogicalTypeId::LIST) {
		vector<string> paths;
		const auto &children = ListValue::GetChildren(constant_arg);
		for (const auto &child : children) {
			if (child.IsNull()) {
				throw BinderException("'variant_keys' does not accept NULL paths");
			}
			paths.push_back(child.GetValue<string>());
		}
		return make_uniq<VariantKeysBindData>(paths);
	} else {
		throw InternalException("Constant-folded argument was not of type VARCHAR");
	}
}

static void CastParameterToVariant(Vector &input, Vector &result, const idx_t count) {
	const auto input_type = input.GetType();

	if (input_type == LogicalType::VARIANT()) {
		result.Reference(input);
		return;
	}

	if (input_type == LogicalType::JSON()) {
		VectorOperations::DefaultTryCast(input, result, count, nullptr);
		return;
	}

	if (input_type == LogicalType::VARCHAR) {
		//! Save on a redundant parse step, and in case of a malformed object let it fail in the cast.
		Vector json_vec(LogicalType::JSON());
		json_vec.Reinterpret(input);
		VectorOperations::DefaultTryCast(json_vec, result, count, nullptr);
		return;
	}

	throw InternalException("Unsupported input type for variant_keys, found: %s", input_type.ToString());
}

static void VariantKeysFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	const auto count = input.size();

	D_ASSERT(input.ColumnCount() == 1 || input.ColumnCount() == 2);

	Vector variant_vec(LogicalType::VARIANT());
	CastParameterToVariant(input.data[0], variant_vec, count);

	if (input.ColumnCount() == 2) {
		const auto &path = input.data[1];
		D_ASSERT(path.GetVectorType() == VectorType::CONSTANT_VECTOR);
		(void)path;
	}

	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &info = func_expr.bind_info->Cast<VariantKeysBindData>();

	// TODO: Instead of setting the input as the first path component, parse based on JSONPath/JSON Pointer.
	if (input.ColumnCount() == 2 && input.data[1].GetType().id() == LogicalTypeId::VARCHAR) {
		UnaryVariantKeys(variant_vec, info.paths[0], result, count);
	} else if (input.ColumnCount() == 2 && input.data[1].GetType().id() == LogicalTypeId::LIST) {
		ManyVariantKeys(variant_vec, info.paths, result, count);
	} else {
		UnaryVariantKeys(variant_vec, {}, result, count);
	}
}

static void AddFunctionsWithParameterType(ScalarFunctionSet &fun_set, const LogicalType &input_type) {
	ScalarFunction variant_keys("variant_keys", {}, LogicalType::LIST(LogicalType::VARCHAR), VariantKeysFunction,
	                            VariantKeysBind, VariantKeysPropagateStats);

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
	AddFunctionsWithParameterType(fun_set, LogicalType::VARCHAR);
	// AddFunctionsWithParameterType(fun_set, LogicalType::JSON());

	return fun_set;
}

} // namespace duckdb
