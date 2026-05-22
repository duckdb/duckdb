#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/types/variant.hpp"
#include "duckdb/function/scalar/variant_functions.hpp"
#include "duckdb/function/scalar/variant_utils.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

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

static void PrepareKeyListStorage(Vector &key_ids, const ValidityMask &object_validity,
                                  const VariantNestedData *nested_data, const idx_t count) {
	const auto &list_validity = FlatVector::Validity(key_ids);

	idx_t total_key_count = 0;
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		if (!list_validity.RowIsValid(row_idx) || !object_validity.RowIsValid(row_idx)) {
			continue;
		}
		total_key_count += nested_data[row_idx].child_count;
	}

	ListVector::Reserve(key_ids, total_key_count);
	ListVector::SetListSize(key_ids, total_key_count);
}

static void WriteKeyIdList(const UnifiedVariantVectorData &variant, Vector &key_ids, VariantNestedData *nested_data,
                           const ValidityMask &object_validity, const idx_t count) {
	PrepareKeyListStorage(key_ids, object_validity, nested_data, count);

	const auto list_entries = FlatVector::GetDataMutable<list_entry_t>(key_ids);
	auto &key_id_child = ListVector::GetChildMutable(key_ids);
	const auto key_ids_data = FlatVector::GetDataMutable<idx_t>(key_id_child);

	idx_t current_offset = 0;
	for (idx_t row_idx = 0; row_idx < count; row_idx++) {
		auto &entry = list_entries[row_idx];
		entry.offset = current_offset;
		entry.length = 0;

		if (!object_validity.RowIsValid(row_idx)) {
			// No keys to write: the row is either NULL or the target is not an object type
			continue;
		}

		const auto &[child_count, children_idx] = nested_data[row_idx];
		entry.length = child_count;

		for (idx_t child_idx = 0; child_idx < child_count; child_idx++) {
			const auto key_id = variant.GetKeysIndex(row_idx, children_idx + child_idx);
			key_ids_data[current_offset + child_idx] = key_id;
		}

		current_offset += entry.length;
	}
}

// TODO: Currently collection will always happen on the unshredded variant, introduce a fast path for shredded variants.
static Vector CollectVariantKeys(const UnifiedVariantVectorData &variant,
                                 const vector<VariantPathComponent> &components, const idx_t count) {
	// By row found keys at the requested path in the VARIANT
	Vector key_ids(LogicalType::LIST(LogicalType::UBIGINT), count);
	VariantPathSelection path_selection(count);

	auto &allocator = Allocator::DefaultAllocator();
	auto owned_nested_data = allocator.Allocate(sizeof(VariantNestedData) * count);
	const auto nested_data = reinterpret_cast<VariantNestedData *>(owned_nested_data.get());

	auto &list_validity = FlatVector::ValidityMutable(key_ids);
	TraverseVariantPath(variant, components, count, nested_data, list_validity, path_selection);

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

static unique_ptr<FunctionData> VariantKeysBind(BindScalarFunctionInput &input) {
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
	if (!VariantBindUtils::GetConstantArgument(context, path_expr, constant_arg)) {
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

static void VariantKeysFunction(DataChunk &input, ExpressionState &state, Vector &result) {
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
		UnaryVariantKeys(variant_vec, {}, result, count);
		return;
	}

	D_ASSERT(n_columns == 2);
	const auto &path_type_id = input.data[1].GetType().id();

	if (path_type_id == LogicalTypeId::VARCHAR) {
		UnaryVariantKeys(variant_vec, info.paths[0], result, count);
		return;
	}
	if (path_type_id == LogicalTypeId::LIST) {
		ManyVariantKeys(variant_vec, info.paths, result, count);
		return;
	}
}

static void AddFunctionsWithParameterType(ScalarFunctionSet &fun_set, const LogicalType &input_type) {
	ScalarFunction variant_keys("variant_keys", {}, LogicalType::LIST(LogicalType::VARCHAR), VariantKeysFunction,
	                            VariantKeysBind, nullptr);

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
