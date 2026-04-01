//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/filter/table_filter_selectivity_optional_function
//
//
//===----------------------------------------------------------------------===//

#include "duckdb/planner/filter/table_filter_functions.hpp"
#include "table_filter_function_helpers.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/expression_executor_state.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"

namespace duckdb {

SelectivityOptionalFilterFunctionData::SelectivityOptionalFilterFunctionData(unique_ptr<Expression> child_filter_expr_p,
                                                                             float selectivity_threshold_p,
                                                                             idx_t n_vectors_to_check_p)
    : child_filter_expr(std::move(child_filter_expr_p)), selectivity_threshold(selectivity_threshold_p),
      n_vectors_to_check(n_vectors_to_check_p) {
}

unique_ptr<FunctionData> SelectivityOptionalFilterFunctionData::Copy() const {
	return make_uniq<SelectivityOptionalFilterFunctionData>(child_filter_expr ? child_filter_expr->Copy() : nullptr,
	                                                        selectivity_threshold, n_vectors_to_check);
}

bool SelectivityOptionalFilterFunctionData::Equals(const FunctionData &other_p) const {
	auto &other = other_p.Cast<SelectivityOptionalFilterFunctionData>();
	if (selectivity_threshold != other.selectivity_threshold || n_vectors_to_check != other.n_vectors_to_check) {
		return false;
	}
	if (!child_filter_expr && !other.child_filter_expr) {
		return true;
	}
	if (!child_filter_expr || !other.child_filter_expr) {
		return false;
	}
	return child_filter_expr->Equals(*other.child_filter_expr);
}

static void SelectivityOptionalFilterSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data,
                                               const ScalarFunction &function) {
	if (!bind_data) {
		return;
	}
	auto &data = bind_data->Cast<SelectivityOptionalFilterFunctionData>();
	serializer.WritePropertyWithDefault<unique_ptr<Expression>>(200, "child_filter_expr", data.child_filter_expr);
	serializer.WritePropertyWithDefault<float>(201, "selectivity_threshold", data.selectivity_threshold);
	serializer.WritePropertyWithDefault<idx_t>(202, "n_vectors_to_check", data.n_vectors_to_check);
}

static unique_ptr<FunctionData> SelectivityOptionalFilterDeserialize(Deserializer &deserializer,
                                                                     ScalarFunction &function) {
	auto child_filter_expr = deserializer.ReadPropertyWithDefault<unique_ptr<Expression>>(200, "child_filter_expr");
	auto selectivity_threshold =
	    deserializer.ReadPropertyWithExplicitDefault<float>(201, "selectivity_threshold", 0.5f);
	auto n_vectors_to_check = deserializer.ReadPropertyWithExplicitDefault<idx_t>(202, "n_vectors_to_check", idx_t(6));
	return make_uniq<SelectivityOptionalFilterFunctionData>(std::move(child_filter_expr), selectivity_threshold,
	                                                        n_vectors_to_check);
}

struct SelectivityOptionalFilterLocalState : public FunctionLocalState {
	SelectivityOptionalFilterLocalState(ClientContext &context, const Expression &child_filter_expr,
	                                    idx_t n_vectors_to_check, float selectivity_threshold)
	    : stats(n_vectors_to_check, selectivity_threshold), executor(context, child_filter_expr) {
	}

	bool IsActive() const {
		return stats.IsActive();
	}
	void Update(idx_t accepted, idx_t processed) {
		stats.Update(accepted, processed);
	}

	SelectivityTrackingLocalState stats;
	ExpressionExecutor executor;
};

static unique_ptr<FunctionLocalState> SelectivityOptionalFilterInitLocalState(ExpressionState &state,
                                                                              const BoundFunctionExpression &expr,
                                                                              FunctionData *bind_data) {
	auto &data = bind_data->Cast<SelectivityOptionalFilterFunctionData>();
	if (!data.child_filter_expr) {
		return nullptr;
	}
	return make_uniq<SelectivityOptionalFilterLocalState>(state.GetContext(), *data.child_filter_expr,
	                                                      data.n_vectors_to_check, data.selectivity_threshold);
}

static void SelectivityOptionalFilterFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto local_state_ptr = ExecuteFunctionState::GetFunctionState(state);
	if (!local_state_ptr) {
		SetAllTrue(args, result);
		return;
	}
	auto &local_state = local_state_ptr->Cast<SelectivityOptionalFilterLocalState>();
	auto count = args.size();
	ExecuteWithSelectivityTracking(args, result, &local_state, [&] {
		SelectionVector child_sel(count);
		auto approved_count = local_state.executor.SelectExpression(args, child_sel);
		SelectionToBooleanResult(count, child_sel, approved_count, result);
		return approved_count;
	});
}

ScalarFunction SelectivityOptionalFilterScalarFun::GetFunction(const LogicalType &input_type) {
	ScalarFunction func(NAME, {input_type}, LogicalType::BOOLEAN, SelectivityOptionalFilterFunction,
	                    TableFilterFunctions::Bind);
	func.SetInitStateCallback(SelectivityOptionalFilterInitLocalState);
	func.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	func.SetFilterPruneCallback(SelectivityOptionalFilterScalarFun::FilterPrune);
	func.serialize = SelectivityOptionalFilterSerialize;
	func.deserialize = SelectivityOptionalFilterDeserialize;
	return func;
}

FilterPropagateResult SelectivityOptionalFilterScalarFun::FilterPrune(const FunctionStatisticsPruneInput &input) {
	if (!input.bind_data) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	auto &data = input.bind_data->Cast<SelectivityOptionalFilterFunctionData>();
	if (!data.child_filter_expr) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	return ExpressionFilter::CheckExpressionStatistics(*data.child_filter_expr, input.stats);
}

string SelectivityOptionalFilterScalarFun::ToString(const string &child_filter_string) {
	return FormatOptionalFilterString(child_filter_string);
}

ScalarFunction TableFilterSelectivityOptionalFun::GetFunction() {
	return SelectivityOptionalFilterScalarFun::GetFunction(LogicalType::ANY);
}

} // namespace duckdb
