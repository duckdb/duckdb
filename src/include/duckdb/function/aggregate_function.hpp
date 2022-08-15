//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/aggregate_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/function.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/statistics/node_statistics.hpp"
#include "duckdb/planner/bound_result_modifier.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/common/vector_operations/aggregate_executor.hpp"

namespace duckdb {

class BoundAggregateExpression;

struct AggregateInputData {
	AggregateInputData(FunctionData *bind_data_p) : bind_data(bind_data_p) {};
	FunctionData *bind_data;
};

//! The type used for sizing hashed aggregate function states
typedef idx_t (*aggregate_size_t)();
//! The type used for initializing hashed aggregate function states
typedef void (*aggregate_initialize_t)(data_ptr_t state);
//! The type used for updating hashed aggregate functions
typedef void (*aggregate_update_t)(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count,
                                   Vector &state, idx_t count);
//! The type used for combining hashed aggregate states
typedef void (*aggregate_combine_t)(Vector &state, Vector &combined, AggregateInputData &aggr_input_data, idx_t count);
//! The type used for finalizing hashed aggregate function payloads
typedef void (*aggregate_finalize_t)(Vector &state, AggregateInputData &aggr_input_data, Vector &result, idx_t count,
                                     idx_t offset);
//! The type used for propagating statistics in aggregate functions (optional)
typedef unique_ptr<BaseStatistics> (*aggregate_statistics_t)(ClientContext &context, BoundAggregateExpression &expr,
                                                             FunctionData *bind_data,
                                                             vector<unique_ptr<BaseStatistics>> &child_stats,
                                                             NodeStatistics *node_stats);
//! Binds the scalar function and creates the function data
typedef unique_ptr<FunctionData> (*bind_aggregate_function_t)(ClientContext &context, AggregateFunction &function,
                                                              vector<unique_ptr<Expression>> &arguments);
//! The type used for the aggregate destructor method. NOTE: this method is used in destructors and MAY NOT throw.
typedef void (*aggregate_destructor_t)(Vector &state, idx_t count);

//! The type used for updating simple (non-grouped) aggregate functions
typedef void (*aggregate_simple_update_t)(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count,
                                          data_ptr_t state, idx_t count);

//! The type used for updating complex windowed aggregate functions (optional)
typedef std::pair<idx_t, idx_t> FrameBounds;
typedef void (*aggregate_window_t)(Vector inputs[], const ValidityMask &filter_mask,
                                   AggregateInputData &aggr_input_data, idx_t input_count, data_ptr_t state,
                                   const FrameBounds &frame, const FrameBounds &prev, Vector &result, idx_t rid,
                                   idx_t bias);

class AggregateFunction : public BaseScalarFunction {
public:
	DUCKDB_API AggregateFunction(const string &name, const vector<LogicalType> &arguments,
	                             const LogicalType &return_type, aggregate_size_t state_size,
	                             aggregate_initialize_t initialize, aggregate_update_t update,
	                             aggregate_combine_t combine, aggregate_finalize_t finalize,
	                             FunctionNullHandling null_handling = FunctionNullHandling::DEFAULT_NULL_HANDLING,
	                             aggregate_simple_update_t simple_update = nullptr,
	                             bind_aggregate_function_t bind = nullptr, aggregate_destructor_t destructor = nullptr,
	                             aggregate_statistics_t statistics = nullptr, aggregate_window_t window = nullptr)
	    : BaseScalarFunction(name, arguments, return_type, FunctionSideEffects::NO_SIDE_EFFECTS,
	                         LogicalType(LogicalTypeId::INVALID), null_handling),
	      state_size(state_size), initialize(initialize), update(update), combine(combine), finalize(finalize),
	      simple_update(simple_update), window(window), bind(bind), destructor(destructor), statistics(statistics) {
	}

	DUCKDB_API AggregateFunction(const string &name, const vector<LogicalType> &arguments,
	                             const LogicalType &return_type, aggregate_size_t state_size,
	                             aggregate_initialize_t initialize, aggregate_update_t update,
	                             aggregate_combine_t combine, aggregate_finalize_t finalize,
	                             aggregate_simple_update_t simple_update = nullptr,
	                             bind_aggregate_function_t bind = nullptr, aggregate_destructor_t destructor = nullptr,
	                             aggregate_statistics_t statistics = nullptr, aggregate_window_t window = nullptr)
	    : BaseScalarFunction(name, arguments, return_type, FunctionSideEffects::NO_SIDE_EFFECTS,
	                         LogicalType(LogicalTypeId::INVALID)),
	      state_size(state_size), initialize(initialize), update(update), combine(combine), finalize(finalize),
	      simple_update(simple_update), window(window), bind(bind), destructor(destructor), statistics(statistics) {
	}

	DUCKDB_API AggregateFunction(const vector<LogicalType> &arguments, const LogicalType &return_type,
	                             aggregate_size_t state_size, aggregate_initialize_t initialize,
	                             aggregate_update_t update, aggregate_combine_t combine, aggregate_finalize_t finalize,
	                             FunctionNullHandling null_handling = FunctionNullHandling::DEFAULT_NULL_HANDLING,
	                             aggregate_simple_update_t simple_update = nullptr,
	                             bind_aggregate_function_t bind = nullptr, aggregate_destructor_t destructor = nullptr,
	                             aggregate_statistics_t statistics = nullptr, aggregate_window_t window = nullptr)
	    : AggregateFunction(string(), arguments, return_type, state_size, initialize, update, combine, finalize,
	                        null_handling, simple_update, bind, destructor, statistics, window) {
	}

	DUCKDB_API AggregateFunction(const vector<LogicalType> &arguments, const LogicalType &return_type,
	                             aggregate_size_t state_size, aggregate_initialize_t initialize,
	                             aggregate_update_t update, aggregate_combine_t combine, aggregate_finalize_t finalize,
	                             aggregate_simple_update_t simple_update = nullptr,
	                             bind_aggregate_function_t bind = nullptr, aggregate_destructor_t destructor = nullptr,
	                             aggregate_statistics_t statistics = nullptr, aggregate_window_t window = nullptr)
	    : AggregateFunction(string(), arguments, return_type, state_size, initialize, update, combine, finalize,
	                        FunctionNullHandling::DEFAULT_NULL_HANDLING, simple_update, bind, destructor, statistics,
	                        window) {
	}
	//! The hashed aggregate state sizing function
	aggregate_size_t state_size;
	//! The hashed aggregate state initialization function
	aggregate_initialize_t initialize;
	//! The hashed aggregate update state function
	aggregate_update_t update;
	//! The hashed aggregate combine states function
	aggregate_combine_t combine;
	//! The hashed aggregate finalization function
	aggregate_finalize_t finalize;
	//! The simple aggregate update function (may be null)
	aggregate_simple_update_t simple_update;
	//! The windowed aggregate frame update function (may be null)
	aggregate_window_t window;

	//! The bind function (may be null)
	bind_aggregate_function_t bind;
	//! The destructor method (may be null)
	aggregate_destructor_t destructor;

	//! The statistics propagation function (may be null)
	aggregate_statistics_t statistics;

	DUCKDB_API bool operator==(const AggregateFunction &rhs) const {
		return state_size == rhs.state_size && initialize == rhs.initialize && update == rhs.update &&
		       combine == rhs.combine && finalize == rhs.finalize && window == rhs.window;
	}
	DUCKDB_API bool operator!=(const AggregateFunction &rhs) const {
		return !(*this == rhs);
	}

	DUCKDB_API static unique_ptr<BoundAggregateExpression>
	BindAggregateFunction(ClientContext &context, AggregateFunction bound_function,
	                      vector<unique_ptr<Expression>> children, unique_ptr<Expression> filter = nullptr,
	                      bool is_distinct = false, unique_ptr<BoundOrderModifier> order_bys = nullptr);

	DUCKDB_API static unique_ptr<FunctionData> BindSortedAggregate(AggregateFunction &bound_function,
	                                                               vector<unique_ptr<Expression>> &children,
	                                                               unique_ptr<FunctionData> bind_info,
	                                                               unique_ptr<BoundOrderModifier> order_bys);

public:
	template <class STATE, class RESULT_TYPE, class OP>
	static AggregateFunction NullaryAggregate(LogicalType return_type) {
		return AggregateFunction(
		    {}, return_type, AggregateFunction::StateSize<STATE>, AggregateFunction::StateInitialize<STATE, OP>,
		    AggregateFunction::NullaryScatterUpdate<STATE, OP>, AggregateFunction::StateCombine<STATE, OP>,
		    AggregateFunction::StateFinalize<STATE, RESULT_TYPE, OP>, AggregateFunction::NullaryUpdate<STATE, OP>);
	}

	template <class STATE, class INPUT_TYPE, class RESULT_TYPE, class OP>
	static AggregateFunction
	UnaryAggregate(const LogicalType &input_type, LogicalType return_type,
	               FunctionNullHandling null_handling = FunctionNullHandling::DEFAULT_NULL_HANDLING) {
		return AggregateFunction(
		    {input_type}, return_type, AggregateFunction::StateSize<STATE>,
		    AggregateFunction::StateInitialize<STATE, OP>, AggregateFunction::UnaryScatterUpdate<STATE, INPUT_TYPE, OP>,
		    AggregateFunction::StateCombine<STATE, OP>, AggregateFunction::StateFinalize<STATE, RESULT_TYPE, OP>,
		    null_handling, AggregateFunction::UnaryUpdate<STATE, INPUT_TYPE, OP>);
	}

	template <class STATE, class INPUT_TYPE, class RESULT_TYPE, class OP>
	static AggregateFunction UnaryAggregateDestructor(LogicalType input_type, LogicalType return_type) {
		auto aggregate = UnaryAggregate<STATE, INPUT_TYPE, RESULT_TYPE, OP>(input_type, return_type);
		aggregate.destructor = AggregateFunction::StateDestroy<STATE, OP>;
		return aggregate;
	}

	template <class STATE, class A_TYPE, class B_TYPE, class RESULT_TYPE, class OP>
	static AggregateFunction BinaryAggregate(const LogicalType &a_type, const LogicalType &b_type,
	                                         LogicalType return_type) {
		return AggregateFunction({a_type, b_type}, return_type, AggregateFunction::StateSize<STATE>,
		                         AggregateFunction::StateInitialize<STATE, OP>,
		                         AggregateFunction::BinaryScatterUpdate<STATE, A_TYPE, B_TYPE, OP>,
		                         AggregateFunction::StateCombine<STATE, OP>,
		                         AggregateFunction::StateFinalize<STATE, RESULT_TYPE, OP>,
		                         AggregateFunction::BinaryUpdate<STATE, A_TYPE, B_TYPE, OP>);
	}

public:
	template <class STATE>
	static idx_t StateSize() {
		return sizeof(STATE);
	}

	template <class STATE, class OP>
	static void StateInitialize(data_ptr_t state) {
		OP::Initialize((STATE *)state);
	}

	template <class STATE, class OP>
	static void NullaryScatterUpdate(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count,
	                                 Vector &states, idx_t count) {
		D_ASSERT(input_count == 0);
		AggregateExecutor::NullaryScatter<STATE, OP>(states, aggr_input_data, count);
	}

	template <class STATE, class OP>
	static void NullaryUpdate(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count, data_ptr_t state,
	                          idx_t count) {
		D_ASSERT(input_count == 0);
		AggregateExecutor::NullaryUpdate<STATE, OP>(state, aggr_input_data, count);
	}

	template <class STATE, class T, class OP>
	static void UnaryScatterUpdate(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count,
	                               Vector &states, idx_t count) {
		D_ASSERT(input_count == 1);
		AggregateExecutor::UnaryScatter<STATE, T, OP>(inputs[0], states, aggr_input_data, count);
	}

	template <class STATE, class INPUT_TYPE, class OP>
	static void UnaryUpdate(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count, data_ptr_t state,
	                        idx_t count) {
		D_ASSERT(input_count == 1);
		AggregateExecutor::UnaryUpdate<STATE, INPUT_TYPE, OP>(inputs[0], aggr_input_data, state, count);
	}

	template <class STATE, class INPUT_TYPE, class RESULT_TYPE, class OP>
	static void UnaryWindow(Vector inputs[], const ValidityMask &filter_mask, AggregateInputData &aggr_input_data,
	                        idx_t input_count, data_ptr_t state, const FrameBounds &frame, const FrameBounds &prev,
	                        Vector &result, idx_t rid, idx_t bias) {
		D_ASSERT(input_count == 1);
		AggregateExecutor::UnaryWindow<STATE, INPUT_TYPE, RESULT_TYPE, OP>(inputs[0], filter_mask, aggr_input_data,
		                                                                   state, frame, prev, result, rid, bias);
	}

	template <class STATE, class A_TYPE, class B_TYPE, class OP>
	static void BinaryScatterUpdate(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count,
	                                Vector &states, idx_t count) {
		D_ASSERT(input_count == 2);
		AggregateExecutor::BinaryScatter<STATE, A_TYPE, B_TYPE, OP>(aggr_input_data, inputs[0], inputs[1], states,
		                                                            count);
	}

	template <class STATE, class A_TYPE, class B_TYPE, class OP>
	static void BinaryUpdate(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count, data_ptr_t state,
	                         idx_t count) {
		D_ASSERT(input_count == 2);
		AggregateExecutor::BinaryUpdate<STATE, A_TYPE, B_TYPE, OP>(aggr_input_data, inputs[0], inputs[1], state, count);
	}

	template <class STATE, class OP>
	static void StateCombine(Vector &source, Vector &target, AggregateInputData &aggr_input_data, idx_t count) {
		AggregateExecutor::Combine<STATE, OP>(source, target, aggr_input_data, count);
	}

	template <class STATE, class RESULT_TYPE, class OP>
	static void StateFinalize(Vector &states, AggregateInputData &aggr_input_data, Vector &result, idx_t count,
	                          idx_t offset) {
		AggregateExecutor::Finalize<STATE, RESULT_TYPE, OP>(states, aggr_input_data, result, count, offset);
	}

	template <class STATE, class OP>
	static void StateDestroy(Vector &states, idx_t count) {
		AggregateExecutor::Destroy<STATE, OP>(states, count);
	}
};

} // namespace duckdb
