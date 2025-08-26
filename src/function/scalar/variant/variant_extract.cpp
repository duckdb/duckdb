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
	component.lookup_mode = VariantChildLookupMode::BY_INDEX;
	component.index = index;
}

unique_ptr<FunctionData> BindData::Copy() const {
	if (component.lookup_mode == VariantChildLookupMode::BY_INDEX) {
		return make_uniq<BindData>(component.index);
	}
	return make_uniq<BindData>(component.key);
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
	auto &variant = input.data[0];
	D_ASSERT(variant.GetType() == LogicalType::VARIANT());

	auto &path = input.data[1];
	D_ASSERT(path.GetVectorType() == VectorType::CONSTANT_VECTOR);

	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	auto &info = func_expr.bind_info->Cast<BindData>();
	auto &allocator = Allocator::DefaultAllocator();

	RecursiveUnifiedVectorFormat source_format;
	Vector::RecursiveToUnifiedFormat(variant, count, source_format);

	//! Path either contains array indices or object keys
	SelectionVector value_index_sel;
	value_index_sel.Initialize(count);
	for (idx_t i = 0; i < count; i++) {
		value_index_sel[i] = 0;
	}

	SelectionVector new_value_index_sel;
	new_value_index_sel.Initialize(count);

	auto owned_nested_data = allocator.Allocate(sizeof(VariantNestedData) * count);
	auto nested_data = reinterpret_cast<VariantNestedData *>(owned_nested_data.get());

	string error;
	auto &component = info.component;

	auto expected_type = component.lookup_mode == VariantChildLookupMode::BY_INDEX ? VariantLogicalType::ARRAY
	                                                                               : VariantLogicalType::OBJECT;
	if (!VariantUtils::CollectNestedData(source_format, expected_type, value_index_sel, count, optional_idx(),
	                                     nested_data, error)) {
		throw InvalidInputException(error);
	}

	if (!VariantUtils::FindChildValues(source_format, component, optional_idx(), new_value_index_sel, nested_data,
	                                   count)) {
		switch (component.lookup_mode) {
		case VariantChildLookupMode::BY_INDEX: {
			throw InvalidInputException("ARRAY does not contain the index: %d", component.index);
		}
		case VariantChildLookupMode::BY_KEY: {
			throw InvalidInputException("OBJECT does not contain the key: %s", component.key);
		}
		}
	}

	//! We have these indices left, the simplest way we can finalize this is:
	//! Leave the 'keys' alone, we'll just potentially have unused keys
	//! Leave the 'children' alone, we'll have less children, but the child indices are still correct
	//! We can also leave 'data' alone
	//! We just need to remap index 0 of the 'values' list (for all rows)

	auto &values = UnifiedVariantVector::GetValues(source_format);
	auto values_data = values.GetData<list_entry_t>(values);
	auto &raw_values = VariantVector::GetValues(variant);
	auto values_list_size = ListVector::GetListSize(raw_values);

	//! Create a new Variant that references the existing data of the input Variant
	result.Initialize(false, count);
	VariantVector::GetKeys(result).Reference(VariantVector::GetKeys(variant));
	VariantVector::GetChildren(result).Reference(VariantVector::GetChildren(variant));
	VariantVector::GetData(result).Reference(VariantVector::GetData(variant));

	//! Copy the existing 'values'
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
		auto &list_entry = values_data[values.sel->get_index(i)];
		new_sel.set_index(list_entry.offset, list_entry.offset + new_value_index_sel[i]);
	}

	auto &result_type_id = VariantVector::GetValuesTypeId(result);
	auto &result_byte_offset = VariantVector::GetValuesByteOffset(result);
	result_type_id.Dictionary(VariantVector::GetValuesTypeId(variant), values_list_size, new_sel, values_list_size);
	result_byte_offset.Dictionary(VariantVector::GetValuesByteOffset(variant), values_list_size, new_sel,
	                              values_list_size);

	auto value_is_null = VariantUtils::ValueIsNull(source_format, new_value_index_sel, count, optional_idx());
	for (idx_t i = 0; i < value_is_null.size(); i++) {
		if (value_is_null[i]) {
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
