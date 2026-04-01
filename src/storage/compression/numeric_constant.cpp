#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/function/compression/compression.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/storage/segment/uncompressed.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"

namespace duckdb {

static optional_ptr<const BoundFunctionExpression> TryGetFunctionExpression(const Expression &expression) {
	if (expression.GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
		return nullptr;
	}
	return expression.Cast<BoundFunctionExpression>();
}

static bool IsSimpleFilterColumnRef(const Expression &expression) {
	return expression.type == ExpressionType::BOUND_REF ||
	       expression.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF;
}

static bool TryComparisonFiltersNullValues(const BoundComparisonExpression &comparison, bool &filters_nulls,
                                           bool &filters_valid_values) {
	optional_ptr<const BoundConstantExpression> constant_expr;
	auto comparison_type = comparison.GetExpressionType();
	if (IsSimpleFilterColumnRef(*comparison.left) && comparison.right->type == ExpressionType::VALUE_CONSTANT) {
		constant_expr = comparison.right->Cast<BoundConstantExpression>();
	} else if (IsSimpleFilterColumnRef(*comparison.right) && comparison.left->type == ExpressionType::VALUE_CONSTANT) {
		constant_expr = comparison.left->Cast<BoundConstantExpression>();
		comparison_type = FlipComparisonExpression(comparison_type);
	} else {
		return false;
	}
	if (constant_expr->value.IsNull()) {
		switch (comparison_type) {
		case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
			filters_valid_values = true;
			break;
		case ExpressionType::COMPARE_DISTINCT_FROM:
			filters_nulls = true;
			break;
		default:
			filters_nulls = true;
			filters_valid_values = true;
			break;
		}
	} else {
		switch (comparison_type) {
		case ExpressionType::COMPARE_DISTINCT_FROM:
			filters_nulls = false;
			break;
		default:
			filters_nulls = true;
			break;
		}
	}
	return true;
}

static bool TryExpressionFiltersNullValues(const Expression &expression, bool &filters_nulls,
                                           bool &filters_valid_values) {
	filters_nulls = false;
	filters_valid_values = false;

	if (expression.GetExpressionClass() == ExpressionClass::BOUND_CONJUNCTION) {
		auto &conjunction = expression.Cast<BoundConjunctionExpression>();
		if (conjunction.type == ExpressionType::CONJUNCTION_AND) {
			for (auto &child : conjunction.children) {
				bool child_filters_nulls = false;
				bool child_filters_valid_values = false;
				if (!TryExpressionFiltersNullValues(*child, child_filters_nulls, child_filters_valid_values)) {
					return false;
				}
				filters_nulls = filters_nulls || child_filters_nulls;
				filters_valid_values = filters_valid_values || child_filters_valid_values;
			}
			return true;
		}
		if (conjunction.type == ExpressionType::CONJUNCTION_OR) {
			filters_nulls = true;
			filters_valid_values = true;
			for (auto &child : conjunction.children) {
				bool child_filters_nulls = false;
				bool child_filters_valid_values = false;
				if (!TryExpressionFiltersNullValues(*child, child_filters_nulls, child_filters_valid_values)) {
					return false;
				}
				filters_nulls = filters_nulls && child_filters_nulls;
				filters_valid_values = filters_valid_values && child_filters_valid_values;
			}
			return true;
		}
		return false;
	}

	if (expression.GetExpressionClass() == ExpressionClass::BOUND_COMPARISON) {
		auto &comparison = expression.Cast<BoundComparisonExpression>();
		return TryComparisonFiltersNullValues(comparison, filters_nulls, filters_valid_values);
	}

	if (expression.GetExpressionClass() == ExpressionClass::BOUND_OPERATOR) {
		auto &op = expression.Cast<BoundOperatorExpression>();
		if (op.children.size() != 1 || !IsSimpleFilterColumnRef(*op.children[0])) {
			return false;
		}
		switch (expression.type) {
		case ExpressionType::OPERATOR_IS_NULL:
			filters_valid_values = true;
			return true;
		case ExpressionType::OPERATOR_IS_NOT_NULL:
			filters_nulls = true;
			return true;
		default:
			return false;
		}
	}

	auto func_expr = TryGetFunctionExpression(expression);
	if (!func_expr) {
		return false;
	}

	auto &function_name = func_expr->function.name;
	if (function_name == OptionalFilterScalarFun::NAME) {
		return true;
	}
	if (function_name == BloomFilterScalarFun::NAME) {
		if (!func_expr->bind_info) {
			return true;
		}
		auto &data = func_expr->bind_info->Cast<BloomFilterFunctionData>();
		if (!data.filter) {
			return true;
		}
		filters_nulls = data.filters_null_values;
		return true;
	}
	if (function_name == SelectivityOptionalFilterScalarFun::NAME) {
		if (!func_expr->bind_info) {
			return false;
		}
		auto &data = func_expr->bind_info->Cast<SelectivityOptionalFilterFunctionData>();
		if (!data.child_filter_expr) {
			return false;
		}
		return TryExpressionFiltersNullValues(*data.child_filter_expr, filters_nulls, filters_valid_values);
	}
	if (function_name == PerfectHashJoinScalarFun::NAME) {
		if (!func_expr->bind_info) {
			return true;
		}
		auto &data = func_expr->bind_info->Cast<PerfectHashJoinFunctionData>();
		if (!data.executor) {
			return true;
		}
		filters_nulls = true;
		return true;
	}
	if (function_name == PrefixRangeScalarFun::NAME) {
		if (!func_expr->bind_info) {
			return true;
		}
		auto &data = func_expr->bind_info->Cast<PrefixRangeFunctionData>();
		if (!data.filter || !data.filter->IsInitialized()) {
			return true;
		}
		filters_nulls = true;
		return true;
	}
	if (function_name == DynamicFilterScalarFun::NAME) {
		if (!func_expr->bind_info) {
			return true;
		}
		auto &data = func_expr->bind_info->Cast<DynamicFilterFunctionData>();
		if (!data.filter_data || !data.filter_data->initialized.load()) {
			return true;
		}
		filters_nulls = true;
		return true;
	}
	return false;
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
unique_ptr<SegmentScanState> ConstantInitScan(const QueryContext &context, ColumnSegment &segment) {
	return nullptr;
}

//===--------------------------------------------------------------------===//
// Scan Partial
//===--------------------------------------------------------------------===//
void ConstantFillFunctionValidity(ColumnSegment &segment, Vector &result, idx_t start_idx, idx_t count) {
	auto &stats = segment.stats.statistics;
	if (stats.CanHaveNull()) {
		auto &mask = FlatVector::Validity(result);
		for (idx_t i = 0; i < count; i++) {
			mask.SetInvalid(start_idx + i);
		}
	}
}

template <class T>
void ConstantFillFunction(ColumnSegment &segment, Vector &result, idx_t start_idx, idx_t count) {
	auto &nstats = segment.stats.statistics;

	auto data = FlatVector::GetData<T>(result);
	auto constant_value = NumericStats::GetMin<T>(nstats);
	for (idx_t i = 0; i < count; i++) {
		data[start_idx + i] = constant_value;
	}
}

void ConstantScanPartialValidity(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
                                 idx_t result_offset) {
	ConstantFillFunctionValidity(segment, result, result_offset, scan_count);
}

template <class T>
void ConstantScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
                         idx_t result_offset) {
	ConstantFillFunction<T>(segment, result, result_offset, scan_count);
}

//===--------------------------------------------------------------------===//
// Scan base data
//===--------------------------------------------------------------------===//
void ConstantScanFunctionValidity(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result) {
	auto &stats = segment.stats.statistics;
	if (stats.CanHaveNull()) {
		if (result.GetType().InternalType() == PhysicalType::STRUCT ||
		    result.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(result, true);
		} else {
			result.Flatten(scan_count);
			ConstantFillFunctionValidity(segment, result, 0, scan_count);
		}
	}
}

template <class T>
void ConstantScanFunction(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result) {
	auto &nstats = segment.stats.statistics;

	auto data = FlatVector::GetData<T>(result);
	data[0] = NumericStats::GetMin<T>(nstats);
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
}

//===--------------------------------------------------------------------===//
// Fetch
//===--------------------------------------------------------------------===//
void ConstantFetchRowValidity(ColumnSegment &segment, ColumnFetchState &state, row_t row_id, Vector &result,
                              idx_t result_idx) {
	ConstantFillFunctionValidity(segment, result, result_idx, 1);
}

template <class T>
void ConstantFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id, Vector &result, idx_t result_idx) {
	ConstantFillFunction<T>(segment, result, result_idx, 1);
}

//===--------------------------------------------------------------------===//
// Select
//===--------------------------------------------------------------------===//
void ConstantSelectValidity(ColumnSegment &segment, ColumnScanState &state, idx_t vector_count, Vector &result,
                            const SelectionVector &sel, idx_t sel_count) {
	ConstantScanFunctionValidity(segment, state, sel_count, result);
}

template <class T>
void ConstantSelect(ColumnSegment &segment, ColumnScanState &state, idx_t vector_count, Vector &result,
                    const SelectionVector &sel, idx_t sel_count) {
	ConstantScanFunction<T>(segment, state, vector_count, result);
}

//===--------------------------------------------------------------------===//
// Filter
//===--------------------------------------------------------------------===//
void ConstantFun::FiltersNullValues(const LogicalType &type, const TableFilter &filter, bool &filters_nulls,
                                    bool &filters_valid_values, TableFilterState &filter_state) {
	filters_nulls = false;
	filters_valid_values = false;

	auto &expr_filter = ExpressionFilter::GetExpressionFilter(filter, "ConstantFun::FiltersNullValues");
	auto &state = filter_state.Cast<ExpressionFilterState>();
	if (!TryExpressionFiltersNullValues(*expr_filter.expr, filters_nulls, filters_valid_values)) {
		Value val(type);
		//! If the expression evaluates to true, containing only a NULL vector, it *must* be an IS NULL filter
		filters_nulls = !expr_filter.EvaluateWithConstant(*state.executor, val);
		filters_valid_values = false;
	}
}

void ConstantFilterValidity(ColumnSegment &segment, ColumnScanState &state, idx_t vector_count, Vector &result,
                            SelectionVector &sel, idx_t &sel_count, const TableFilter &filter,
                            TableFilterState &filter_state) {
	// check what effect the filter has on NULL values
	bool filters_nulls, filters_valid_values;
	ConstantFun::FiltersNullValues(result.GetType(), filter, filters_nulls, filters_valid_values, filter_state);

	auto &stats = segment.stats.statistics;
	if (stats.CanHaveNull()) {
		// all values are NULL
		if (filters_nulls) {
			// ... and the filter removes NULL values
			sel_count = 0;
			return;
		}
	} else {
		// all values are valid
		if (filters_valid_values) {
			// ... and the filter removes valid values
			sel_count = 0;
			return;
		}
	}
	ConstantScanFunctionValidity(segment, state, vector_count, result);
}

//===--------------------------------------------------------------------===//
// Get Function
//===--------------------------------------------------------------------===//
CompressionFunction ConstantGetFunctionValidity(PhysicalType data_type) {
	D_ASSERT(data_type == PhysicalType::BIT);
	return CompressionFunction(CompressionType::COMPRESSION_CONSTANT, data_type, nullptr, nullptr, nullptr, nullptr,
	                           nullptr, nullptr, ConstantInitScan, ConstantScanFunctionValidity,
	                           ConstantScanPartialValidity, ConstantFetchRowValidity, UncompressedFunctions::EmptySkip,
	                           nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
	                           ConstantSelectValidity, ConstantFilterValidity);
}

template <class T>
CompressionFunction ConstantGetFunction(PhysicalType data_type) {
	return CompressionFunction(CompressionType::COMPRESSION_CONSTANT, data_type, nullptr, nullptr, nullptr, nullptr,
	                           nullptr, nullptr, ConstantInitScan, ConstantScanFunction<T>, ConstantScanPartial<T>,
	                           ConstantFetchRow<T>, UncompressedFunctions::EmptySkip, nullptr, nullptr, nullptr,
	                           nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, ConstantSelect<T>);
}

CompressionFunction ConstantFun::GetFunction(PhysicalType data_type) {
	switch (data_type) {
	case PhysicalType::BIT:
		return ConstantGetFunctionValidity(data_type);
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return ConstantGetFunction<int8_t>(data_type);
	case PhysicalType::INT16:
		return ConstantGetFunction<int16_t>(data_type);
	case PhysicalType::INT32:
		return ConstantGetFunction<int32_t>(data_type);
	case PhysicalType::INT64:
		return ConstantGetFunction<int64_t>(data_type);
	case PhysicalType::UINT8:
		return ConstantGetFunction<uint8_t>(data_type);
	case PhysicalType::UINT16:
		return ConstantGetFunction<uint16_t>(data_type);
	case PhysicalType::UINT32:
		return ConstantGetFunction<uint32_t>(data_type);
	case PhysicalType::UINT64:
		return ConstantGetFunction<uint64_t>(data_type);
	case PhysicalType::INT128:
		return ConstantGetFunction<hugeint_t>(data_type);
	case PhysicalType::UINT128:
		return ConstantGetFunction<uhugeint_t>(data_type);
	case PhysicalType::FLOAT:
		return ConstantGetFunction<float>(data_type);
	case PhysicalType::DOUBLE:
		return ConstantGetFunction<double>(data_type);
	default:
		throw InternalException("Unsupported type for ConstantUncompressed::GetFunction");
	}
}

bool ConstantFun::TypeIsSupported(const PhysicalType physical_type) {
	switch (physical_type) {
	case PhysicalType::BIT:
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::INT64:
	case PhysicalType::UINT8:
	case PhysicalType::UINT16:
	case PhysicalType::UINT32:
	case PhysicalType::UINT64:
	case PhysicalType::INT128:
	case PhysicalType::UINT128:
	case PhysicalType::FLOAT:
	case PhysicalType::DOUBLE:
		return true;
	default:
		return false;
	}
}

} // namespace duckdb
