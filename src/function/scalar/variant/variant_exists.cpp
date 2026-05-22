#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/types/variant.hpp"
#include "duckdb/function/scalar/variant_functions.hpp"
#include "duckdb/function/scalar/variant_utils.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

#include <duckdb/execution/expression_executor.hpp>

namespace duckdb {

struct VariantKeysBindData : public FunctionData {
public:
	explicit VariantKeysBindData();
	explicit VariantKeysBindData(const string &input_path);
	explicit VariantKeysBindData(const vector<string> &input_paths);
	VariantKeysBindData(const VariantKeysBindData &other) = default;

public:
	unique_ptr<FunctionData> Copy() const override;
	bool Equals(const FunctionData &other) const override;

public:
	vector<vector<VariantPathComponent>> paths;
};

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
	for (const auto &path : input_paths) {
		if (path.empty()) {
			paths.emplace_back();
		} else {
			paths.push_back({VariantPathComponent(path)});
		}
	}
}

unique_ptr<FunctionData> VariantKeysBindData::Copy() const {
	return make_uniq<VariantKeysBindData>(*this);
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
			if (paths[i][j] != bind_data.paths[i][j]) {
				return false;
			}
		}
	}
	return true;
}

struct VariantPathSelection {
	explicit VariantPathSelection(const idx_t count) {
		value_index_sel.Initialize(count);
		new_value_index_sel.Initialize(count);

		// We start at values[0] for every row.
		for (idx_t i = 0; i < count; i++) {
			value_index_sel[i] = 0;
		}
	}

	SelectionVector &Input(const idx_t depth) {
		return depth % 2 == 0 ? value_index_sel : new_value_index_sel;
	}

	SelectionVector &Output(const idx_t depth) {
		return depth % 2 == 0 ? new_value_index_sel : value_index_sel;
	}

	//! Input and output buffers used during the object walk, switched per iteration
	SelectionVector value_index_sel, new_value_index_sel;
};

static void TraverseVariantPath(const UnifiedVariantVectorData &variant, const vector<VariantPathComponent> &components,
                                const idx_t count, VariantNestedData *nested_data, ValidityMask &validity,
                                VariantPathSelection &path_selection) {
	for (idx_t i = 0; i < components.size(); i++) {
		auto &component = components[i];
		auto &input_indices = path_selection.Input(i);
		auto &output_indices = path_selection.Output(i);

		if (component.lookup_mode == VariantChildLookupMode::BY_INDEX) {
			throw InternalException("'variant_keys' does not support path indexes");
		}

		(void)VariantUtils::CollectNestedData(variant, VariantLogicalType::OBJECT, input_indices, count, optional_idx(),
		                                      0, nested_data, validity);

		ValidityMask lookup_validity(count);
		VariantUtils::FindChildValues(variant, component, nullptr, output_indices, lookup_validity, nested_data,
		                              validity, count);

		for (idx_t j = 0; j < count; j++) {
			if (!validity.RowIsValid(j)) {
				continue;
			}
			if (lookup_validity.CanHaveNull() && !lookup_validity.RowIsValid(j)) {
				validity.SetInvalid(j);
			}
		}
	}
}

// TODO: Currently collection will always happen on the unshredded variant, introduce a fast path for shredded variants.
static ValidityMask CollectVariantExistence(const UnifiedVariantVectorData &variant,
                                            const vector<VariantPathComponent> &components, const idx_t count) {
	ValidityMask path_validity(count);
	VariantPathSelection path_selection(count);

	auto &allocator = Allocator::DefaultAllocator();
	auto owned_nested_data = allocator.Allocate(sizeof(VariantNestedData) * count);
	const auto nested_data = reinterpret_cast<VariantNestedData *>(owned_nested_data.get());

	TraverseVariantPath(variant, components, count, nested_data, path_validity, path_selection);

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

static vector<string> CollectPaths(const Value &constant_arg) {
	vector<string> paths;
	const auto &children = ListValue::GetChildren(constant_arg);
	for (const auto &child : children) {
		if (child.IsNull()) {
			throw BinderException("'variant_keys' does not accept NULL paths");
		}
		paths.push_back(child.GetValue<string>());
	}

	return paths;
}

// TODO: Remove
static bool GetConstantArgument(ClientContext &context, const Expression &expr, Value &constant_arg) {
	if (!expr.IsFoldable()) {
		return false;
	}
	constant_arg = ExpressionExecutor::EvaluateScalar(context, expr);
	if (!constant_arg.IsNull()) {
		return true;
	}
	return false;
}

static unique_ptr<FunctionData> VariantExistsBind(BindScalarFunctionInput &input) {
	auto &context = input.GetClientContext();
	auto &arguments = input.GetArguments();
	if (arguments.size() != 1 && arguments.size() != 2) {
		throw BinderException("'variant_keys' expects either one VARIANT column argument, or two VARIANT column and "
		                      "VARCHAR path arguments");
	}

	if (arguments.size() == 1) {
		// No path supplied, execute function over the root of the variant
		return make_uniq<VariantKeysBindData>();
	}

	const auto &path_expr = *arguments[1];
	const auto &return_type = path_expr.GetReturnType();
	if (return_type.id() != LogicalTypeId::VARCHAR && return_type.id() != LogicalTypeId::LIST) {
		throw BinderException("'variant_keys' expects the second argument to be of type VARCHAR or VARCHAR[], not %s",
		                      return_type.ToString());
	}
	if (return_type.id() == LogicalTypeId::LIST) {
		const auto child_type_id = ListType::GetChildType(return_type).id();
		if (child_type_id != LogicalTypeId::VARCHAR && child_type_id != LogicalTypeId::SQLNULL) {
			throw BinderException(
			    "'variant_keys' expects the second argument to be of type VARCHAR or VARCHAR[], not %s",
			    return_type.ToString());
		}
	}

	Value constant_arg;
	if (!GetConstantArgument(context, path_expr, constant_arg)) {
		throw BinderException("'variant_keys' expects the second argument to be a constant expression");
	}

	const auto path_type_id = constant_arg.type().id();
	if (path_type_id == LogicalTypeId::VARCHAR) {
		return make_uniq<VariantKeysBindData>(constant_arg.GetValue<string>());
	}
	if (path_type_id == LogicalTypeId::LIST) {
		return make_uniq<VariantKeysBindData>(CollectPaths(constant_arg));
	}

	throw BinderException("'variant_keys' received an unexpected type for the second argument");
}

static void VariantExistsFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	D_ASSERT(input.ColumnCount() == 1 || input.ColumnCount() == 2);
	const auto count = input.size();
	const auto &variant_vec = input.data[0];

	if (input.ColumnCount() == 2) {
		const auto &path = input.data[1];
		D_ASSERT(path.GetVectorType() == VectorType::CONSTANT_VECTOR);
		(void)path;
	}

	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &info = func_expr.bind_info->Cast<VariantKeysBindData>();
	auto n_columns = input.ColumnCount();

	if (n_columns == 1) {
		UnaryVariantExists(variant_vec, {}, result, count);
		return;
	}

	D_ASSERT(n_columns == 2);
	const auto &path_type_id = input.data[1].GetType().id();

	if (path_type_id == LogicalTypeId::VARCHAR) {
		UnaryVariantExists(variant_vec, info.paths[0], result, count);
		return;
	}
	if (path_type_id == LogicalTypeId::LIST) {
		ManyVariantExists(variant_vec, info.paths, result, count);
		return;
	}
}

ScalarFunctionSet VariantExistsFun::GetFunctions() {
	ScalarFunctionSet fun_set;


	ScalarFunction variant_exists("variant_exists", {LogicalType::VARIANT(), LogicalType::VARCHAR},
	                              LogicalType::BOOLEAN, VariantExistsFunction, VariantExistsBind, nullptr);
	fun_set.AddFunction(variant_exists);

	variant_exists.GetSignature().GetParameter(1).SetType(LogicalType::LIST(LogicalType::VARCHAR));
	variant_exists.SetReturnType(LogicalType::LIST(LogicalType::BOOLEAN));
	fun_set.AddFunction(variant_exists);

	return fun_set;
}

} // namespace duckdb
