#include "duckdb/function/scalar/variant_utils.hpp"
#include "duckdb/function/scalar/variant_functions.hpp"
#include "duckdb/function/scalar/regexp.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/execution/expression_executor.hpp"

namespace duckdb {

namespace {

struct BindData : public FunctionData {
public:
	explicit BindData(const string &str);
	explicit BindData(uint32_t index);
	BindData(const BindData &other) = default;

public:
	unique_ptr<FunctionData> Copy() const override;
	bool Equals(const FunctionData &other) const override;

public:
	VariantPathComponent component;
};

} // namespace

BindData::BindData(const string &str) : FunctionData() {
	component.lookup_mode = VariantChildLookupMode::BY_KEY;
	component.key = str;
}
BindData::BindData(uint32_t index) : FunctionData() {
	if (index == 0) {
		throw BinderException("Extracting index 0 from VARIANT(ARRAY) is invalid, indexes are 1-based");
	}
	component.lookup_mode = VariantChildLookupMode::BY_INDEX;
	component.index = index - 1;
}

unique_ptr<FunctionData> BindData::Copy() const {
	return make_uniq<BindData>(*this);
}

bool BindData::Equals(const FunctionData &other) const {
	auto &bind_data = other.Cast<BindData>();
	if (bind_data.component.lookup_mode != component.lookup_mode) {
		return false;
	}
	if (bind_data.component.lookup_mode == VariantChildLookupMode::BY_INDEX &&
	    bind_data.component.index != component.index) {
		return false;
	}
	if (bind_data.component.lookup_mode == VariantChildLookupMode::BY_KEY && bind_data.component.key != component.key) {
		return false;
	}
	return true;
}

static bool GetConstantArgument(ClientContext &context, Expression &expr, Value &constant_arg) {
	if (!expr.IsFoldable()) {
		return false;
	}
	constant_arg = ExpressionExecutor::EvaluateScalar(context, expr);
	if (!constant_arg.IsNull()) {
		return true;
	}
	return false;
}

static unique_ptr<FunctionData> VariantExtractBind(ClientContext &context, ScalarFunction &bound_function,
                                                   vector<unique_ptr<Expression>> &arguments) {
	if (arguments.size() != 2) {
		throw BinderException("'variant_extract' expects two arguments, VARIANT column and VARCHAR path");
	}
	auto &path = *arguments[1];
	if (path.return_type.id() != LogicalTypeId::VARCHAR && path.return_type.id() != LogicalTypeId::UINTEGER) {
		throw BinderException("'variant_extract' expects the second argument to be of type VARCHAR or UINTEGER, not %s",
		                      path.return_type.ToString());
	}

	Value constant_arg;
	if (!GetConstantArgument(context, path, constant_arg)) {
		throw BinderException("'variant_extract' expects the second argument to be a constant expression");
	}

	if (constant_arg.type().id() == LogicalTypeId::VARCHAR) {
		return make_uniq<BindData>(constant_arg.GetValue<string>());
	} else if (constant_arg.type().id() == LogicalTypeId::UINTEGER) {
		return make_uniq<BindData>(constant_arg.GetValue<uint32_t>());
	} else {
		throw InternalException("Constant-folded argument was not of type UINTEGER or VARCHAR");
	}
}

//! FIXME: it could make sense to allow a third argument: 'default'
//! This can currently be achieved with COALESCE(TRY(<extract method>), 'default')
static void VariantExtractFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	auto count = input.size();

	D_ASSERT(input.ColumnCount() == 2);
	auto &variant_vec = input.data[0];
	D_ASSERT(variant_vec.GetType() == LogicalType::VARIANT());

	auto &path = input.data[1];
	D_ASSERT(path.GetVectorType() == VectorType::CONSTANT_VECTOR);
	(void)path;

	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &info = func_expr.bind_info->Cast<BindData>();
	auto &allocator = Allocator::DefaultAllocator();

	RecursiveUnifiedVectorFormat source_format;
	Vector::RecursiveToUnifiedFormat(variant_vec, count, source_format);

	UnifiedVariantVectorData variant(source_format);

	//! Extract always starts by looking at value_index 0
	SelectionVector value_index_sel;
	value_index_sel.Initialize(count);
	for (idx_t i = 0; i < count; i++) {
		value_index_sel[i] = 0;
	}

	SelectionVector new_value_index_sel;
	new_value_index_sel.Initialize(count);

	auto owned_nested_data = allocator.Allocate(sizeof(VariantNestedData) * count);
	auto nested_data = reinterpret_cast<VariantNestedData *>(owned_nested_data.get());

	auto &component = info.component;
	auto expected_type = component.lookup_mode == VariantChildLookupMode::BY_INDEX ? VariantLogicalType::ARRAY
	                                                                               : VariantLogicalType::OBJECT;
	auto collection_result = VariantUtils::CollectNestedData(
	    variant, expected_type, value_index_sel, count, optional_idx(), 0, nested_data, FlatVector::Validity(result));
	if (!collection_result.success) {
		if (expected_type == VariantLogicalType::ARRAY) {
			throw InvalidInputException("Can't extract index %d from a VARIANT(%s)", component.index,
			                            EnumUtil::ToString(collection_result.wrong_type));
		} else {
			D_ASSERT(expected_type == VariantLogicalType::OBJECT);
			throw InvalidInputException("Can't extract key '%s' from a VARIANT(%s)", component.key,
			                            EnumUtil::ToString(collection_result.wrong_type));
		}
	}

	//! Look up the value_index of the child we're extracting
	ValidityMask lookup_validity(count);
	VariantUtils::FindChildValues(variant, component, nullptr, new_value_index_sel, lookup_validity, nested_data,
	                              count);
	if (!lookup_validity.AllValid()) {
		optional_idx index;
		for (idx_t i = 0; i < count; i++) {
			if (!lookup_validity.RowIsValid(i)) {
				index = i;
				break;
			}
		}
		D_ASSERT(index.IsValid());
		switch (component.lookup_mode) {
		case VariantChildLookupMode::BY_INDEX: {
			auto nested_index = index.GetIndex();
			throw InvalidInputException("VARIANT(ARRAY(%d)) is missing index %d", nested_data[nested_index].child_count,
			                            component.index);
		}
		case VariantChildLookupMode::BY_KEY: {
			auto nested_index = index.GetIndex();
			auto row_index = nested_index;
			auto object_keys = VariantUtils::GetObjectKeys(variant, row_index, nested_data[nested_index]);
			throw InvalidInputException("VARIANT(OBJECT(%s)) is missing key '%s'", StringUtil::Join(object_keys, ","),
			                            component.key);
		}
		}
	}

	//! We reference the input, creating a dictionary over the 'values' list entry to remap what value_index 0 points
	//! to. This way we can perform a zero-copy extract on the variant column (when there are no nulls). The following
	//! code looks complicated but is necessary to avoid modifying the 'input'

	auto &values = UnifiedVariantVector::GetValues(source_format);
	auto values_data = values.GetData<list_entry_t>(values);
	auto &raw_values = VariantVector::GetValues(variant_vec);
	auto values_list_size = ListVector::GetListSize(raw_values);

	//! Create a new Variant that references the existing data of the input Variant
	result.Initialize(false, count);
	VariantVector::GetKeys(result).Reference(VariantVector::GetKeys(variant_vec));
	VariantVector::GetChildren(result).Reference(VariantVector::GetChildren(variant_vec));
	VariantVector::GetData(result).Reference(VariantVector::GetData(variant_vec));

	//! Copy the existing 'values' list entry data
	auto &result_values = VariantVector::GetValues(result);
	result_values.Initialize(false, count);
	ListVector::Reserve(result_values, values_list_size);
	ListVector::SetListSize(result_values, values_list_size);
	auto result_values_data = FlatVector::GetData<list_entry_t>(result_values);
	for (idx_t i = 0; i < count; i++) {
		result_values_data[i] = values_data[values.sel->get_index(i)];
	}

	//! Prepare the selection vector to remap index 0 of each row
	SelectionVector new_sel(0, values_list_size);
	for (idx_t i = 0; i < count; i++) {
		if (nested_data[i].is_null) {
			continue;
		}
		auto &list_entry = values_data[values.sel->get_index(i)];
		new_sel.set_index(list_entry.offset, list_entry.offset + new_value_index_sel[i]);
	}

	auto &result_type_id = VariantVector::GetValuesTypeId(result);
	auto &result_byte_offset = VariantVector::GetValuesByteOffset(result);

	result_type_id.Dictionary(VariantVector::GetValuesTypeId(variant_vec), values_list_size, new_sel, values_list_size);
	result_byte_offset.Dictionary(VariantVector::GetValuesByteOffset(variant_vec), values_list_size, new_sel,
	                              values_list_size);

	auto value_is_null = VariantUtils::ValueIsNull(variant, new_value_index_sel, count, optional_idx());
	if (!value_is_null.empty()) {
		result.Flatten(count);
		for (auto &i : value_is_null) {
			FlatVector::SetNull(result, i, true);
		}
	}

	if (input.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

ScalarFunctionSet VariantExtractFun::GetFunctions() {
	auto variant_type = LogicalType::VARIANT();

	ScalarFunctionSet fun_set;
	fun_set.AddFunction(ScalarFunction("variant_extract", {variant_type, LogicalType::VARCHAR}, variant_type,
	                                   VariantExtractFunction, VariantExtractBind));
	fun_set.AddFunction(ScalarFunction("variant_extract", {variant_type, LogicalType::UINTEGER}, variant_type,
	                                   VariantExtractFunction, VariantExtractBind));
	return fun_set;
}

} // namespace duckdb
