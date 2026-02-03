#include "duckdb/function/scalar/variant_utils.hpp"
#include "duckdb/function/scalar/variant_functions.hpp"
#include "duckdb/function/scalar/regexp.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/storage/statistics/struct_stats.hpp"
#include "duckdb/storage/statistics/list_stats.hpp"

namespace duckdb {

VariantExtractBindData::VariantExtractBindData(const string &str) : FunctionData(), component(str) {
}
VariantExtractBindData::VariantExtractBindData(uint32_t index) : FunctionData() {
	if (index == 0) {
		throw BinderException("Extracting index 0 from VARIANT(ARRAY) is invalid, indexes are 1-based");
	}
	component = VariantPathComponent(index - 1);
}

unique_ptr<FunctionData> VariantExtractBindData::Copy() const {
	return make_uniq<VariantExtractBindData>(*this);
}

bool VariantExtractBindData::Equals(const FunctionData &other) const {
	auto &bind_data = other.Cast<VariantExtractBindData>();
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

static unique_ptr<BaseStatistics> VariantExtractPropagateStats(ClientContext &context, FunctionStatisticsInput &input) {
	auto &child_stats = input.child_stats;
	auto &bind_data = input.bind_data;

	auto &info = bind_data->Cast<VariantExtractBindData>();
	auto &variant_stats = child_stats[0];
	const bool is_shredded = VariantStats::IsShredded(variant_stats);
	if (!is_shredded) {
		return nullptr;
	}
	auto &shredded_stats = VariantStats::GetShreddedStats(variant_stats);
	if (!VariantShreddedStats::IsFullyShredded(shredded_stats)) {
		return nullptr;
	}
	auto found_stats = VariantShreddedStats::FindChildStats(shredded_stats, info.component);
	if (!found_stats || !VariantShreddedStats::IsFullyShredded(*found_stats)) {
		return nullptr;
	}

	return VariantStats::WrapExtractedFieldAsVariant(variant_stats, *found_stats);
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
		return make_uniq<VariantExtractBindData>(constant_arg.GetValue<string>());
	} else if (constant_arg.type().id() == LogicalTypeId::UINTEGER) {
		return make_uniq<VariantExtractBindData>(constant_arg.GetValue<uint32_t>());
	} else {
		throw InternalException("Constant-folded argument was not of type UINTEGER or VARCHAR");
	}
}

void VariantUtils::VariantExtract(Vector &variant_vec, const vector<VariantPathComponent> &components, Vector &result,
                                  idx_t count) {
	auto &allocator = Allocator::DefaultAllocator();

	RecursiveUnifiedVectorFormat source_format;
	Vector::RecursiveToUnifiedFormat(variant_vec, count, source_format);

	UnifiedVariantVectorData variant(source_format);

	//! Extract always starts by looking at value_index 0
	SelectionVector value_index_sel;
	value_index_sel.Initialize(count);

	SelectionVector new_value_index_sel;
	new_value_index_sel.Initialize(count);

	for (idx_t i = 0; i < count; i++) {
		value_index_sel[i] = 0;
	}

	auto owned_nested_data = allocator.Allocate(sizeof(VariantNestedData) * count);
	auto nested_data = reinterpret_cast<VariantNestedData *>(owned_nested_data.get());

	//! Perform the extract
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
			if (!lookup_validity.AllValid() && !lookup_validity.RowIsValid(j)) {
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
	auto &result_values_validity = FlatVector::Validity(result_values);
	for (idx_t i = 0; i < count; i++) {
		if (!validity.RowIsValid(i)) {
			result_values_validity.SetInvalid(i);
			continue;
		}
		result_values_data[i] = values_data[values.sel->get_index(i)];
	}

	auto &result_indices = components.size() % 2 == 0 ? value_index_sel : new_value_index_sel;

	//! Prepare the selection vector to remap index 0 of each row
	SelectionVector new_sel(0, values_list_size);
	for (idx_t i = 0; i < count; i++) {
		if (!validity.RowIsValid(i)) {
			continue;
		}
		auto &list_entry = values_data[values.sel->get_index(i)];
		new_sel.set_index(list_entry.offset, list_entry.offset + result_indices[i]);
	}

	auto &result_type_id = VariantVector::GetValuesTypeId(result);
	auto &result_byte_offset = VariantVector::GetValuesByteOffset(result);

	result_type_id.Dictionary(VariantVector::GetValuesTypeId(variant_vec), values_list_size, new_sel, values_list_size);
	result_byte_offset.Dictionary(VariantVector::GetValuesByteOffset(variant_vec), values_list_size, new_sel,
	                              values_list_size);

	if (!validity.AllValid()) {
		//! Create a copy of the vector, because we used Reference before, and we now need to adjust the data
		//! Which is a problem if we're still sharing the memory with 'input'
		Vector other(result.GetType(), count);
		VectorOperations::Copy(result, other, count, 0, 0);
		result.Reference(other);

		for (idx_t i = 0; i < count; i++) {
			if (!validity.RowIsValid(i)) {
				FlatVector::SetNull(result, i, true);
			}
		}
	}

	if (variant_vec.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
	result.Verify(count);
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
	auto &info = func_expr.bind_info->Cast<VariantExtractBindData>();
	VariantUtils::VariantExtract(variant_vec, {info.component}, result, count);
}

ScalarFunctionSet VariantExtractFun::GetFunctions() {
	auto variant_type = LogicalType::VARIANT();

	ScalarFunctionSet fun_set;
	ScalarFunction variant_extract("variant_extract", {}, variant_type, VariantExtractFunction, VariantExtractBind,
	                               nullptr, VariantExtractPropagateStats);

	variant_extract.arguments = {variant_type, LogicalType::VARCHAR};
	fun_set.AddFunction(variant_extract);

	variant_extract.arguments = {variant_type, LogicalType::UINTEGER};
	fun_set.AddFunction(variant_extract);
	return fun_set;
}

} // namespace duckdb
