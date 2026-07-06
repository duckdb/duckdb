#include "duckdb/planner/table_filter_state.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"

namespace duckdb {

static unique_ptr<ExpressionFilterExecutor> TryCreateFastExecutor(const Expression &expression,
                                                                  bool inside_selectivity_optional);

static void InitializeExecutor(ClientContext &context, const Expression &expression, ExpressionFilterState &state) {
	state.executor = make_uniq<ExpressionExecutor>(context);
	state.executor->AddExpression(expression);
}

ExpressionFilterState::ExpressionFilterState(ClientContext &context, const Expression &expression) {
	fast_executor = TryCreateFastExecutor(expression, false);
	InitializeExecutor(context, expression, *this);
}

ExpressionFilterState::~ExpressionFilterState() {
}

class ConstantFalseFilterExecutor final : public ExpressionFilterExecutor {
public:
	idx_t FilterSelection(SelectionVector &sel, Vector &vector, idx_t scan_count,
	                      idx_t &approved_tuple_count) override {
		approved_tuple_count = 0;
		return 0;
	}
};

template <class T, class OP, bool HAS_NULL>
static idx_t TemplatedFilterSelection(const UnifiedVectorFormat &vdata, T predicate, const SelectionVector &sel,
                                      const idx_t approved_tuple_count, SelectionVector &result_sel) {
	auto &mask = vdata.validity;
	const auto vec = UnifiedVectorFormat::GetData<const T>(vdata);
	idx_t result_count = 0;
	for (idx_t i = 0; i < approved_tuple_count; i++) {
		const auto idx = sel.get_index(i);
		auto vector_idx = vdata.sel->get_index(idx);
		bool comparison_result =
		    (!HAS_NULL || mask.RowIsValidUnsafe(vector_idx)) && OP::Operation(vec[vector_idx], predicate);
		result_sel.set_index(result_count, idx);
		result_count += comparison_result;
	}
	return result_count;
}

template <class T>
static void FilterSelectionSwitch(UnifiedVectorFormat &vdata, T predicate, SelectionVector &sel,
                                  idx_t &approved_tuple_count, ExpressionType comparison_type) {
	SelectionVector new_sel(approved_tuple_count);
	auto &mask = vdata.validity;
	switch (comparison_type) {
	case ExpressionType::COMPARE_EQUAL:
		if (mask.CannotHaveNull()) {
			approved_tuple_count =
			    TemplatedFilterSelection<T, Equals, false>(vdata, predicate, sel, approved_tuple_count, new_sel);
		} else {
			approved_tuple_count =
			    TemplatedFilterSelection<T, Equals, true>(vdata, predicate, sel, approved_tuple_count, new_sel);
		}
		break;
	case ExpressionType::COMPARE_NOTEQUAL:
		if (mask.CannotHaveNull()) {
			approved_tuple_count =
			    TemplatedFilterSelection<T, NotEquals, false>(vdata, predicate, sel, approved_tuple_count, new_sel);
		} else {
			approved_tuple_count =
			    TemplatedFilterSelection<T, NotEquals, true>(vdata, predicate, sel, approved_tuple_count, new_sel);
		}
		break;
	case ExpressionType::COMPARE_LESSTHAN:
		if (mask.CannotHaveNull()) {
			approved_tuple_count =
			    TemplatedFilterSelection<T, LessThan, false>(vdata, predicate, sel, approved_tuple_count, new_sel);
		} else {
			approved_tuple_count =
			    TemplatedFilterSelection<T, LessThan, true>(vdata, predicate, sel, approved_tuple_count, new_sel);
		}
		break;
	case ExpressionType::COMPARE_GREATERTHAN:
		if (mask.CannotHaveNull()) {
			approved_tuple_count =
			    TemplatedFilterSelection<T, GreaterThan, false>(vdata, predicate, sel, approved_tuple_count, new_sel);
		} else {
			approved_tuple_count =
			    TemplatedFilterSelection<T, GreaterThan, true>(vdata, predicate, sel, approved_tuple_count, new_sel);
		}
		break;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		if (mask.CannotHaveNull()) {
			approved_tuple_count = TemplatedFilterSelection<T, LessThanEquals, false>(vdata, predicate, sel,
			                                                                          approved_tuple_count, new_sel);
		} else {
			approved_tuple_count =
			    TemplatedFilterSelection<T, LessThanEquals, true>(vdata, predicate, sel, approved_tuple_count, new_sel);
		}
		break;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		if (mask.CannotHaveNull()) {
			approved_tuple_count = TemplatedFilterSelection<T, GreaterThanEquals, false>(vdata, predicate, sel,
			                                                                             approved_tuple_count, new_sel);
		} else {
			approved_tuple_count = TemplatedFilterSelection<T, GreaterThanEquals, true>(vdata, predicate, sel,
			                                                                            approved_tuple_count, new_sel);
		}
		break;
	default:
		throw NotImplementedException("Unknown comparison type for table filter");
	}
	sel.Initialize(new_sel);
}

template <class T>
class ComparisonFilterExecutor final : public ExpressionFilterExecutor {
public:
	ComparisonFilterExecutor(T predicate_p, ExpressionType comparison_type_p)
	    : predicate(predicate_p), comparison_type(comparison_type_p) {
	}

	idx_t FilterSelection(SelectionVector &sel, Vector &vector, idx_t scan_count,
	                      idx_t &approved_tuple_count) override {
		if (approved_tuple_count == 0) {
			return 0;
		}
		UnifiedVectorFormat vdata;
		vector.ToUnifiedFormat(vdata);
		FilterSelectionSwitch<T>(vdata, predicate, sel, approved_tuple_count, comparison_type);
		return approved_tuple_count;
	}

private:
	T predicate;
	ExpressionType comparison_type;
};

class ConjunctionAndFilterExecutor final : public ExpressionFilterExecutor {
public:
	explicit ConjunctionAndFilterExecutor(vector<unique_ptr<ExpressionFilterExecutor>> children_p)
	    : children(std::move(children_p)) {
	}

	idx_t FilterSelection(SelectionVector &sel, Vector &vector, idx_t scan_count,
	                      idx_t &approved_tuple_count) override {
		for (auto &child : children) {
			child->FilterSelection(sel, vector, scan_count, approved_tuple_count);
			if (approved_tuple_count == 0) {
				break;
			}
		}
		return approved_tuple_count;
	}

private:
	vector<unique_ptr<ExpressionFilterExecutor>> children;
};

class OptionalFilterExecutor final : public ExpressionFilterExecutor {
public:
	idx_t FilterSelection(SelectionVector &sel, Vector &vector, idx_t scan_count,
	                      idx_t &approved_tuple_count) override {
		return approved_tuple_count;
	}
};

class SelectivityOptionalFilterExecutor final : public ExpressionFilterExecutor {
public:
	SelectivityOptionalFilterExecutor(unique_ptr<ExpressionFilterExecutor> child_p, idx_t n_vectors_to_check,
	                                  float selectivity_threshold)
	    : child(std::move(child_p)), stats(n_vectors_to_check, selectivity_threshold) {
	}

	idx_t FilterSelection(SelectionVector &sel, Vector &vector, idx_t scan_count,
	                      idx_t &approved_tuple_count) override {
		if (approved_tuple_count == 0) {
			return 0;
		}
		if (!child) {
			return approved_tuple_count;
		}
		if (!stats.IsActive()) {
			stats.Update(0, 0);
			return approved_tuple_count;
		}
		const auto before_count = approved_tuple_count;
		child->FilterSelection(sel, vector, scan_count, approved_tuple_count);
		stats.Update(approved_tuple_count, before_count);
		return approved_tuple_count;
	}

private:
	unique_ptr<ExpressionFilterExecutor> child;
	SelectivityOptionalFilterState::SelectivityStats stats;
};

class BloomFilterExecutor final : public ExpressionFilterExecutor {
public:
	BloomFilterExecutor(const BloomFilterFunctionData &data, bool inside_selectivity_optional)
	    : filter(data.filter), filters_null_values(data.filters_null_values), keys_sliced(data.key_type),
	      hashes(LogicalType::HASH) {
		if (!inside_selectivity_optional && data.n_vectors_to_check != 0) {
			stats = make_uniq<SelectivityOptionalFilterState::SelectivityStats>(data.n_vectors_to_check,
			                                                                    data.selectivity_threshold);
		}
	}

	idx_t FilterSelection(SelectionVector &sel, Vector &vector, idx_t scan_count,
	                      idx_t &approved_tuple_count) override {
		if (approved_tuple_count == 0) {
			return 0;
		}
		if (!filter || !filter->IsInitialized()) {
			return approved_tuple_count;
		}
		if (stats && !stats->IsActive()) {
			stats->Update(0, 0);
			return approved_tuple_count;
		}

		PrepareCapacity(approved_tuple_count);
		if (sel.IsSet()) {
			keys_sliced.Slice(vector, sel, approved_tuple_count);
			VectorOperations::Hash(keys_sliced, hashes, approved_tuple_count);
		} else {
			VectorOperations::Hash(vector, hashes, approved_tuple_count);
		}
		hashes.Flatten();
		const auto bloom_count = filter->LookupHashes(hashes, bloom_sel, approved_tuple_count);

		auto input = sel.IsSet() ? &keys_sliced : &vector;
		UnifiedVectorFormat input_data;
		input->ToUnifiedFormat(input_data);
		if (input_data.validity.CannotHaveNull()) {
			if (stats) {
				stats->Update(bloom_count, approved_tuple_count);
			}
			TranslateSelection(sel, bloom_count, approved_tuple_count);
			approved_tuple_count = bloom_count;
			return approved_tuple_count;
		}

		idx_t result_count = 0;
		idx_t bloom_idx = 0;
		for (idx_t i = 0; i < approved_tuple_count; i++) {
			const auto matched = bloom_idx < bloom_count && bloom_sel.get_index_unsafe(bloom_idx) == i;
			if (matched) {
				bloom_idx++;
			}
			const auto input_idx = input_data.sel->get_index(i);
			bool passed;
			if (!input_data.validity.RowIsValid(input_idx)) {
				passed = !filters_null_values;
			} else {
				passed = matched;
			}
			if (passed) {
				result_sel.set_index(result_count++, sel.get_index(i));
			}
		}
		if (stats) {
			stats->Update(result_count, approved_tuple_count);
		}
		approved_tuple_count = result_count;
		sel.Initialize(result_sel);
		return approved_tuple_count;
	}

private:
	void PrepareCapacity(idx_t count) {
		if (current_capacity >= count) {
			return;
		}
		hashes.Initialize(VectorDataInitialization::UNINITIALIZED, count);
		bloom_sel.Initialize(count);
		result_sel.Initialize(count);
		current_capacity = count;
	}

	void TranslateSelection(SelectionVector &sel, idx_t result_count, idx_t approved_tuple_count) {
		if (result_count == approved_tuple_count) {
			return;
		}
		if (sel.IsSet()) {
			for (idx_t i = 0; i < result_count; i++) {
				const auto flat_sel_idx = bloom_sel.get_index(i);
				const auto original_sel_idx = sel.get_index(flat_sel_idx);
				sel.set_index(i, original_sel_idx);
			}
		} else {
			sel.Initialize(bloom_sel);
		}
	}

	optional_ptr<BloomFilter> filter;
	bool filters_null_values;
	Vector keys_sliced;
	Vector hashes;
	SelectionVector bloom_sel;
	SelectionVector result_sel;
	idx_t current_capacity = 0;
	unique_ptr<SelectivityOptionalFilterState::SelectivityStats> stats;
};

class PrefixRangeFilterExecutor final : public ExpressionFilterExecutor {
public:
	PrefixRangeFilterExecutor(const PrefixRangeFunctionData &data, bool inside_selectivity_optional)
	    : filter(data.filter), keys_sliced(data.key_type) {
		if (!inside_selectivity_optional && data.n_vectors_to_check != 0) {
			stats = make_uniq<SelectivityOptionalFilterState::SelectivityStats>(data.n_vectors_to_check,
			                                                                    data.selectivity_threshold);
		}
	}

	idx_t FilterSelection(SelectionVector &sel, Vector &vector, idx_t scan_count,
	                      idx_t &approved_tuple_count) override {
		if (approved_tuple_count == 0) {
			return 0;
		}
		if (!filter || !filter->IsInitialized()) {
			return approved_tuple_count;
		}
		if (stats && !stats->IsActive()) {
			stats->Update(0, 0);
			return approved_tuple_count;
		}

		PrepareCapacity(approved_tuple_count);
		if (sel.IsSet()) {
			keys_sliced.Slice(vector, sel, approved_tuple_count);
			const auto result_count = filter->LookupKeys(keys_sliced, local_sel, approved_tuple_count);
			if (result_count != approved_tuple_count) {
				TranslateSelection(sel, result_count);
			}
			if (stats) {
				stats->Update(result_count, approved_tuple_count);
			}
			approved_tuple_count = result_count;
			return approved_tuple_count;
		}

		const auto before_count = approved_tuple_count;
		const auto result_count = filter->LookupKeys(vector, local_sel, approved_tuple_count);
		if (stats) {
			stats->Update(result_count, approved_tuple_count);
		}
		approved_tuple_count = result_count;
		if (result_count != before_count) {
			sel.Initialize(local_sel);
		}
		return approved_tuple_count;
	}

private:
	void PrepareCapacity(idx_t count) {
		if (current_capacity >= count) {
			return;
		}
		local_sel.Initialize(count);
		result_sel.Initialize(count);
		current_capacity = count;
	}

	void TranslateSelection(SelectionVector &sel, idx_t result_count) {
		for (idx_t i = 0; i < result_count; i++) {
			result_sel.set_index(i, sel.get_index(local_sel.get_index(i)));
		}
		sel.Initialize(result_sel);
	}

	optional_ptr<PrefixRangeFilter> filter;
	Vector keys_sliced;
	SelectionVector local_sel;
	SelectionVector result_sel;
	idx_t current_capacity = 0;
	unique_ptr<SelectivityOptionalFilterState::SelectivityStats> stats;
};

static bool IsSupportedComparisonType(ExpressionType comparison_type) {
	switch (comparison_type) {
	case ExpressionType::COMPARE_EQUAL:
	case ExpressionType::COMPARE_NOTEQUAL:
	case ExpressionType::COMPARE_LESSTHAN:
	case ExpressionType::COMPARE_GREATERTHAN:
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return true;
	default:
		return false;
	}
}

template <class T>
static unique_ptr<ExpressionFilterExecutor> CreateComparisonExecutor(const Value &predicate,
                                                                     ExpressionType comparison_type) {
	return make_uniq<ComparisonFilterExecutor<T>>(predicate.GetValueUnsafe<T>(), comparison_type);
}

static unique_ptr<ExpressionFilterExecutor> CreateComparisonExecutor(const LogicalType &type, const Value &predicate,
                                                                     ExpressionType comparison_type) {
	switch (type.id()) {
	case LogicalTypeId::DATE:
		return CreateComparisonExecutor<date_t>(predicate, comparison_type);
	case LogicalTypeId::TIME:
		return CreateComparisonExecutor<dtime_t>(predicate, comparison_type);
	case LogicalTypeId::TIME_NS:
		return CreateComparisonExecutor<dtime_ns_t>(predicate, comparison_type);
	case LogicalTypeId::TIME_TZ:
		return CreateComparisonExecutor<dtime_tz_t>(predicate, comparison_type);
	case LogicalTypeId::TIMESTAMP:
		return CreateComparisonExecutor<timestamp_t>(predicate, comparison_type);
	case LogicalTypeId::TIMESTAMP_SEC:
		return CreateComparisonExecutor<timestamp_sec_t>(predicate, comparison_type);
	case LogicalTypeId::TIMESTAMP_MS:
		return CreateComparisonExecutor<timestamp_ms_t>(predicate, comparison_type);
	case LogicalTypeId::TIMESTAMP_NS:
		return CreateComparisonExecutor<timestamp_ns_t>(predicate, comparison_type);
	case LogicalTypeId::TIMESTAMP_TZ:
		return CreateComparisonExecutor<timestamp_tz_t>(predicate, comparison_type);
	case LogicalTypeId::TIMESTAMP_TZ_NS:
		return CreateComparisonExecutor<timestamp_tz_ns_t>(predicate, comparison_type);
	default:
		break;
	}

	switch (type.InternalType()) {
	case PhysicalType::BOOL:
		return CreateComparisonExecutor<bool>(predicate, comparison_type);
	case PhysicalType::UINT8:
		return CreateComparisonExecutor<uint8_t>(predicate, comparison_type);
	case PhysicalType::UINT16:
		return CreateComparisonExecutor<uint16_t>(predicate, comparison_type);
	case PhysicalType::UINT32:
		return CreateComparisonExecutor<uint32_t>(predicate, comparison_type);
	case PhysicalType::UINT64:
		return CreateComparisonExecutor<uint64_t>(predicate, comparison_type);
	case PhysicalType::UINT128:
		return CreateComparisonExecutor<uhugeint_t>(predicate, comparison_type);
	case PhysicalType::INT8:
		return CreateComparisonExecutor<int8_t>(predicate, comparison_type);
	case PhysicalType::INT16:
		return CreateComparisonExecutor<int16_t>(predicate, comparison_type);
	case PhysicalType::INT32:
		return CreateComparisonExecutor<int32_t>(predicate, comparison_type);
	case PhysicalType::INT64:
		return CreateComparisonExecutor<int64_t>(predicate, comparison_type);
	case PhysicalType::INT128:
		return CreateComparisonExecutor<hugeint_t>(predicate, comparison_type);
	case PhysicalType::FLOAT:
		return CreateComparisonExecutor<float>(predicate, comparison_type);
	case PhysicalType::DOUBLE:
		return CreateComparisonExecutor<double>(predicate, comparison_type);
	default:
		return nullptr;
	}
}

static unique_ptr<ExpressionFilterExecutor> TryCreateComparisonExecutor(const BoundFunctionExpression &comparison) {
	auto comparison_type = comparison.GetExpressionType();
	if (!IsSupportedComparisonType(comparison_type)) {
		return nullptr;
	}

	optional_ptr<const BoundReferenceExpression> reference;
	optional_ptr<const BoundConstantExpression> constant;
	const auto &left = BoundComparisonExpression::Left(comparison);
	const auto &right = BoundComparisonExpression::Right(comparison);
	if (left.GetExpressionClass() == ExpressionClass::BOUND_REF &&
	    right.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
		reference = &left.Cast<BoundReferenceExpression>();
		constant = &right.Cast<BoundConstantExpression>();
	} else if (left.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT &&
	           right.GetExpressionClass() == ExpressionClass::BOUND_REF) {
		reference = &right.Cast<BoundReferenceExpression>();
		constant = &left.Cast<BoundConstantExpression>();
		comparison_type = FlipComparisonExpression(comparison_type);
	} else {
		return nullptr;
	}
	if (reference->Index() != 0) {
		return nullptr;
	}

	auto predicate = constant->GetValue();
	if (predicate.IsNull()) {
		return make_uniq<ConstantFalseFilterExecutor>();
	}

	auto &column_type = reference->GetReturnType();
	if (predicate.type() != column_type) {
		Value cast_predicate;
		if (!predicate.DefaultTryCastAs(column_type, cast_predicate, nullptr)) {
			return nullptr;
		}
		predicate = std::move(cast_predicate);
		if (predicate.IsNull()) {
			return make_uniq<ConstantFalseFilterExecutor>();
		}
	}
	if (predicate.type().InternalType() != column_type.InternalType()) {
		return nullptr;
	}
	return CreateComparisonExecutor(column_type, predicate, comparison_type);
}

static bool IsColumnReferenceFunction(const BoundFunctionExpression &func) {
	auto &children = func.GetChildren();
	if (children.size() != 1 || children[0]->GetExpressionClass() != ExpressionClass::BOUND_REF) {
		return false;
	}
	auto &ref = children[0]->Cast<BoundReferenceExpression>();
	return ref.Index() == 0;
}

static unique_ptr<ExpressionFilterExecutor> TryCreateFunctionExecutor(const BoundFunctionExpression &func,
                                                                      bool inside_selectivity_optional) {
	if (BoundComparisonExpression::IsComparison(func)) {
		return TryCreateComparisonExecutor(func);
	}
	if (!IsColumnReferenceFunction(func)) {
		return nullptr;
	}

	const auto &func_name = func.Function().GetName();
	if (func_name == OptionalFilterScalarFun::NAME) {
		return make_uniq<OptionalFilterExecutor>();
	}
	if (func_name == SelectivityOptionalFilterScalarFun::NAME) {
		if (!func.BindInfo()) {
			return make_uniq<OptionalFilterExecutor>();
		}
		auto &data = func.BindInfo()->Cast<SelectivityOptionalFilterFunctionData>();
		auto child = data.child_filter_expr ? TryCreateFastExecutor(*data.child_filter_expr, true) : nullptr;
		return make_uniq<SelectivityOptionalFilterExecutor>(std::move(child), data.n_vectors_to_check,
		                                                    data.selectivity_threshold);
	}
	if (func_name == BloomFilterScalarFun::NAME) {
		if (!func.BindInfo()) {
			return nullptr;
		}
		return make_uniq<BloomFilterExecutor>(func.BindInfo()->Cast<BloomFilterFunctionData>(),
		                                      inside_selectivity_optional);
	}
	if (func_name == PrefixRangeScalarFun::NAME) {
		if (!func.BindInfo()) {
			return nullptr;
		}
		return make_uniq<PrefixRangeFilterExecutor>(func.BindInfo()->Cast<PrefixRangeFunctionData>(),
		                                            inside_selectivity_optional);
	}
	return nullptr;
}

static unique_ptr<ExpressionFilterExecutor> TryCreateFastExecutor(const Expression &expression,
                                                                  bool inside_selectivity_optional) {
	switch (expression.GetExpressionClass()) {
	case ExpressionClass::BOUND_FUNCTION:
		return TryCreateFunctionExecutor(expression.Cast<BoundFunctionExpression>(), inside_selectivity_optional);
	case ExpressionClass::BOUND_CONJUNCTION: {
		if (expression.GetExpressionType() != ExpressionType::CONJUNCTION_AND) {
			return nullptr;
		}
		auto &conjunction = expression.Cast<BoundConjunctionExpression>();
		vector<unique_ptr<ExpressionFilterExecutor>> children;
		for (auto &child_expr : conjunction.GetChildren()) {
			auto child_executor = TryCreateFastExecutor(*child_expr, inside_selectivity_optional);
			if (!child_executor) {
				return nullptr;
			}
			children.push_back(std::move(child_executor));
		}
		return make_uniq<ConjunctionAndFilterExecutor>(std::move(children));
	}
	default:
		return nullptr;
	}
}

unique_ptr<TableFilterState> TableFilterState::Initialize(ClientContext &context, const TableFilter &filter) {
	auto &expr_filter = ExpressionFilter::GetExpressionFilter(filter, "TableFilterState::Initialize");
	return make_uniq<ExpressionFilterState>(context, *expr_filter.expr);
}

} // namespace duckdb
