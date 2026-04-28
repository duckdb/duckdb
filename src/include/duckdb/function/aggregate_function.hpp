//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/aggregate_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/array.hpp"
#include "duckdb/common/vector_operations/aggregate_executor.hpp"
#include "duckdb/function/aggregate_state.hpp"
#include "duckdb/planner/bound_result_modifier.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {

class BufferManager;
class InterruptState;
class BoundAggregateFunction;

//! A half-open range of frame boundary values _relative to the current row_
//! This is why they are signed values.
struct FrameDelta {
	FrameDelta() : begin(0), end(0) {};
	FrameDelta(int64_t begin, int64_t end) : begin(begin), end(end) {};
	int64_t begin = 0;
	int64_t end = 0;
};

//! The half-open ranges of frame boundary values relative to the current row
using FrameStats = array<FrameDelta, 2>;

//! The partition data for custom window functions
//! Note that if the inputs is nullptr then the column count is 0,
//! but the row count will still be valid
class ColumnDataCollection;
struct WindowPartitionInput {
	WindowPartitionInput(ExecutionContext &context, const ColumnDataCollection *inputs, const idx_t count,
	                     const vector<column_t> &column_ids, const vector<bool> &all_valid,
	                     const ValidityMask &filter_mask, const FrameStats &stats, InterruptState &interrupt_state)
	    : context(context), inputs(inputs), count(count), column_ids(column_ids), all_valid(all_valid),
	      filter_mask(filter_mask), stats(stats), interrupt_state(interrupt_state) {
	}
	ExecutionContext &context;
	const ColumnDataCollection *inputs;
	const idx_t count;
	const vector<column_t> column_ids;
	const vector<bool> &all_valid;
	const ValidityMask &filter_mask;
	const FrameStats stats;
	InterruptState &interrupt_state;
};

class BindAggregateFunctionInput {
public:
	BindAggregateFunctionInput(ClientContext &context_p, BoundAggregateFunction &bound_function_p,
	                           vector<unique_ptr<Expression>> &arguments_p)
	    : context(context_p), bound_function(bound_function_p), arguments(arguments_p) {
	}

	ClientContext &GetClientContext() const {
		return context;
	}
	BoundAggregateFunction &GetBoundFunction() const {
		return bound_function;
	}
	vector<unique_ptr<Expression>> &GetArguments() const {
		return arguments;
	}

private:
	ClientContext &context;
	BoundAggregateFunction &bound_function;
	vector<unique_ptr<Expression>> &arguments;
};

//! The type used for sizing hashed aggregate function states
typedef idx_t (*aggregate_size_t)(const AggregateFunction &function);
//! The type used for initializing hashed aggregate function states
typedef void (*aggregate_initialize_t)(const AggregateFunction &function, data_ptr_t state);
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
                                                             AggregateStatisticsInput &input);
//! Binds the scalar function and creates the function data
typedef unique_ptr<FunctionData> (*bind_aggregate_function_t)(BindAggregateFunctionInput &input);
//! The type used for the aggregate destructor method. NOTE: this method is used in destructors and MAY NOT throw.
typedef void (*aggregate_destructor_t)(Vector &state, AggregateInputData &aggr_input_data, idx_t count);

//! The type used for updating simple (non-grouped) aggregate functions
typedef void (*aggregate_simple_update_t)(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count,
                                          data_ptr_t state, idx_t count);

//! The type used for computing complex/custom windowed aggregate functions (optional)
typedef void (*aggregate_window_t)(AggregateInputData &aggr_input_data, const WindowPartitionInput &partition,
                                   const_data_ptr_t g_state, data_ptr_t l_state, const SubFrames &subframes,
                                   Vector &result, idx_t rid);

//! Batched variant of aggregate_window_t — called once per Evaluate() with frame
//! bounds for all `count` output rows pre-computed. `subframes_per_row` points
//! to `count` SubFrames entries (each 1-3 FrameBounds depending on EXCLUDE
//! clause). When set, the window executor prefers this over the per-row
//! callback, letting implementations issue a single batched call (e.g., one
//! RPC for the whole Evaluate chunk instead of count separate calls).
typedef void (*aggregate_window_batch_t)(AggregateInputData &aggr_input_data, const WindowPartitionInput &partition,
                                         const_data_ptr_t g_state, data_ptr_t l_state,
                                         const SubFrames *subframes_per_row, idx_t count, Vector &result,
                                         idx_t row_idx);

//! The type used for initializing shared complex/custom windowed aggregate state (optional)
typedef void (*aggregate_wininit_t)(AggregateInputData &aggr_input_data, const WindowPartitionInput &partition,
                                    data_ptr_t g_state);

typedef void (*aggregate_serialize_t)(Serializer &serializer, const optional_ptr<FunctionData> bind_data,
                                      const BoundAggregateFunction &function);
typedef unique_ptr<FunctionData> (*aggregate_deserialize_t)(Deserializer &deserializer,
                                                            BoundAggregateFunction &function);

typedef LogicalType (*aggregate_get_state_type_t)(const AggregateFunction &function);

struct AggregateFunctionInfo {
	DUCKDB_API virtual ~AggregateFunctionInfo();

	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<const TARGET &>(*this);
	}
};

enum class AggregateDestructorType {
	STANDARD,
	// legacy destructors allow non-trivial destructors in aggregate states
	// these might not be trivial to off-load to disk
	LEGACY
};

class AggregateFunctionCallbacks {
public:
	//! The hashed aggregate state sizing function
	aggregate_size_t state_size = nullptr;
	//! The hashed aggregate state initialization function
	aggregate_initialize_t initialize = nullptr;
	//! The hashed aggregate update state function (may be null, if window is set)
	aggregate_update_t update = nullptr;
	//! The hashed aggregate combine states function (may be null, if window is set)
	aggregate_combine_t combine = nullptr;
	//! The hashed aggregate finalization function (may be null, if window is set)
	aggregate_finalize_t finalize = nullptr;
	//! The simple aggregate update function (may be null)
	aggregate_simple_update_t simple_update = nullptr;
	//! The windowed aggregate custom function (may be null)
	aggregate_window_t window = nullptr;
	//! The windowed aggregate custom initialization function (may be null)
	aggregate_wininit_t window_init = nullptr;
	//! Batched windowed aggregate function (may be null; preferred when set)
	aggregate_window_batch_t window_batch = nullptr;

	//! The bind function (may be null)
	bind_aggregate_function_t bind = nullptr;

	//! The destructor method (may be null)
	aggregate_destructor_t destructor = nullptr;

	//! The statistics propagation function (may be null)
	aggregate_statistics_t statistics = nullptr;

	aggregate_serialize_t serialize = nullptr;

	aggregate_deserialize_t deserialize = nullptr;

	aggregate_get_state_type_t get_state_type = nullptr;

	bool operator==(const AggregateFunctionCallbacks &rhs) const;
	bool operator!=(const AggregateFunctionCallbacks &rhs) const;
};

class AggregateFunctionProperties : public FunctionProperties {
public:
	//! Whether or not the aggregate is order dependent
	AggregateOrderDependent order_dependent = AggregateOrderDependent::ORDER_DEPENDENT;
	//! Whether or not the aggregate is affect by distinct modifiers
	AggregateDistinctDependent distinct_dependent = AggregateDistinctDependent::DISTINCT_DEPENDENT;

	bool operator==(const AggregateFunctionProperties &rhs) const;
	bool operator!=(const AggregateFunctionProperties &rhs) const;
};

class AggregateFunction : public SimpleFunction { // NOLINT: work-around bug in clang-tidy
public:
	AggregateFunction(const string &name, const vector<LogicalType> &arguments, const LogicalType &return_type,
	                  aggregate_size_t state_size, aggregate_initialize_t initialize, aggregate_update_t update,
	                  aggregate_combine_t combine, aggregate_finalize_t finalize,
	                  FunctionNullHandling null_handling = FunctionNullHandling::DEFAULT_NULL_HANDLING,
	                  aggregate_simple_update_t simple_update = nullptr, bind_aggregate_function_t bind = nullptr,
	                  aggregate_destructor_t destructor = nullptr, aggregate_statistics_t statistics = nullptr,
	                  aggregate_window_t window = nullptr, aggregate_serialize_t serialize = nullptr,
	                  aggregate_deserialize_t deserialize = nullptr)
	    : SimpleFunction(name, arguments, return_type) {
		properties.null_handling = null_handling;

		callbacks.state_size = state_size;
		callbacks.initialize = initialize;
		callbacks.update = update;
		callbacks.combine = combine;
		callbacks.finalize = finalize;
		callbacks.simple_update = simple_update;
		callbacks.window = window;
		callbacks.bind = bind;
		callbacks.destructor = destructor;
		callbacks.statistics = statistics;
		callbacks.serialize = serialize;
		callbacks.deserialize = deserialize;
	}

	AggregateFunction(const string &name, const vector<LogicalType> &arguments, const LogicalType &return_type,
	                  aggregate_size_t state_size, aggregate_initialize_t initialize, aggregate_update_t update,
	                  aggregate_combine_t combine, aggregate_finalize_t finalize,
	                  aggregate_simple_update_t simple_update = nullptr, bind_aggregate_function_t bind = nullptr,
	                  aggregate_destructor_t destructor = nullptr, aggregate_statistics_t statistics = nullptr,
	                  aggregate_window_t window = nullptr, aggregate_serialize_t serialize = nullptr,
	                  aggregate_deserialize_t deserialize = nullptr)
	    : SimpleFunction(name, arguments, return_type) {
		callbacks.state_size = state_size;
		callbacks.initialize = initialize;
		callbacks.update = update;
		callbacks.combine = combine;
		callbacks.finalize = finalize;
		callbacks.simple_update = simple_update;
		callbacks.bind = bind;
		callbacks.destructor = destructor;
		callbacks.statistics = statistics;
		callbacks.window = window;
		callbacks.serialize = serialize;
		callbacks.deserialize = deserialize;
	}

	AggregateFunction(const vector<LogicalType> &arguments, const LogicalType &return_type, aggregate_size_t state_size,
	                  aggregate_initialize_t initialize, aggregate_update_t update, aggregate_combine_t combine,
	                  aggregate_finalize_t finalize,
	                  FunctionNullHandling null_handling = FunctionNullHandling::DEFAULT_NULL_HANDLING,
	                  aggregate_simple_update_t simple_update = nullptr, bind_aggregate_function_t bind = nullptr,
	                  aggregate_destructor_t destructor = nullptr, aggregate_statistics_t statistics = nullptr,
	                  aggregate_window_t window = nullptr, aggregate_serialize_t serialize = nullptr,
	                  aggregate_deserialize_t deserialize = nullptr)
	    : AggregateFunction(string(), arguments, return_type, state_size, initialize, update, combine, finalize,
	                        null_handling, simple_update, bind, destructor, statistics, window, serialize,
	                        deserialize) {
	}

	AggregateFunction(const vector<LogicalType> &arguments, const LogicalType &return_type, aggregate_size_t state_size,
	                  aggregate_initialize_t initialize, aggregate_update_t update, aggregate_combine_t combine,
	                  aggregate_finalize_t finalize, aggregate_simple_update_t simple_update = nullptr,
	                  bind_aggregate_function_t bind = nullptr, aggregate_destructor_t destructor = nullptr,
	                  aggregate_statistics_t statistics = nullptr, aggregate_window_t window = nullptr,
	                  aggregate_serialize_t serialize = nullptr, aggregate_deserialize_t deserialize = nullptr)
	    : AggregateFunction(string(), arguments, return_type, state_size, initialize, update, combine, finalize,
	                        FunctionNullHandling::DEFAULT_NULL_HANDLING, simple_update, bind, destructor, statistics,
	                        window, serialize, deserialize) {
	}

	// Window constructor
	AggregateFunction(const vector<LogicalType> &arguments, const LogicalType &return_type, aggregate_size_t state_size,
	                  aggregate_initialize_t initialize, aggregate_wininit_t window_init, aggregate_window_t window,
	                  bind_aggregate_function_t bind = nullptr, aggregate_destructor_t destructor = nullptr,
	                  aggregate_statistics_t statistics = nullptr, aggregate_serialize_t serialize = nullptr,
	                  aggregate_deserialize_t deserialize = nullptr)
	    : SimpleFunction(name, arguments, return_type) {
		callbacks.state_size = state_size;
		callbacks.initialize = initialize;
		callbacks.window = window;
		callbacks.window_init = window_init;
		callbacks.bind = bind;
		callbacks.destructor = destructor;
		callbacks.statistics = statistics;
		callbacks.serialize = serialize;
		callbacks.deserialize = deserialize;
	}

	// clang-format off
	bool HasBindCallback() const { return callbacks.bind != nullptr; }
	bind_aggregate_function_t GetBindCallback() const { return callbacks.bind; }
	void SetBindCallback(bind_aggregate_function_t callback) { callbacks.bind = callback; }

	unique_ptr<BoundAggregateExpression> Bind(ClientContext &context, vector<unique_ptr<Expression>> arguments) const;

	bool HasStateInitCallback() const { return callbacks.initialize != nullptr; }
	aggregate_initialize_t GetStateInitCallback() const { return callbacks.initialize; }
	void SetStateInitCallback(aggregate_initialize_t callback) { callbacks.initialize = callback; }

	bool HasStateSizeCallback() const { return callbacks.state_size != nullptr; }
	aggregate_size_t GetStateSizeCallback() const { return callbacks.state_size; }
	void SetStateSizeCallback(aggregate_size_t callback) { callbacks.state_size = callback; }

	bool HasStateDestructorCallback() const { return callbacks.destructor != nullptr; }
	aggregate_destructor_t GetStateDestructorCallback() const { return callbacks.destructor; }
	void SetStateDestructorCallback(aggregate_destructor_t callback) { callbacks.destructor = callback; }

	bool HasStateUpdateCallback() const { return callbacks.update != nullptr; }
	aggregate_update_t GetStateUpdateCallback() const { return callbacks.update; }
	void SetStateUpdateCallback(aggregate_update_t callback) { callbacks.update = callback; }

	bool HasStateSimpleUpdateCallback() const { return callbacks.simple_update != nullptr; }
	aggregate_simple_update_t GetStateSimpleUpdateCallback() const { return callbacks.simple_update; }
	void SetStateSimpleUpdateCallback(aggregate_simple_update_t callback) { callbacks.simple_update = callback; }

	void SetStateCombineCallback(aggregate_combine_t callback) { callbacks.combine = callback; }
	aggregate_combine_t GetStateCombineCallback() const { return callbacks.combine; }
	bool HasStateCombineCallback() const { return callbacks.combine != nullptr; }

	void SetStateFinalizeCallback(aggregate_finalize_t callback) { callbacks.finalize = callback; }
	aggregate_finalize_t GetStateFinalizeCallback() const { return callbacks.finalize; }
	bool HasStateFinalizeCallback() const { return callbacks.finalize != nullptr; }

	bool HasWindowCallback() const { return callbacks.window != nullptr; }
	aggregate_window_t GetWindowCallback() const { return callbacks.window; }
	void SetWindowCallback(aggregate_window_t callback) { callbacks.window = callback; }

	void SetWindowInitCallback(aggregate_wininit_t callback) { callbacks.window_init = callback; }
	aggregate_wininit_t GetWindowInitCallback() const { return callbacks.window_init; }
	bool HasWindowInitCallback() const { return callbacks.window_init != nullptr; }

	//! Batched window callback — takes precedence over the per-row window
	//! callback when set. See aggregate_window_batch_t for semantics.
	bool HasWindowBatchCallback() const { return callbacks.window_batch != nullptr; }
	aggregate_window_batch_t GetWindowBatchCallback() const { return callbacks.window_batch; }
	void SetWindowBatchCallback(aggregate_window_batch_t callback) { callbacks.window_batch = callback; }

	bool HasStatisticsCallback() const { return callbacks.statistics != nullptr; }
	aggregate_statistics_t GetStatisticsCallback() const { return callbacks.statistics; }
	void SetStatisticsCallback(aggregate_statistics_t callback) { callbacks.statistics = callback; }

	bool HasSerializationCallbacks() const { return callbacks.serialize != nullptr && callbacks.deserialize != nullptr; }
	void SetSerializeCallback(aggregate_serialize_t callback) { callbacks.serialize = callback; }
	void SetDeserializeCallback(aggregate_deserialize_t callback) { callbacks.deserialize = callback; }
	aggregate_serialize_t GetSerializeCallback() const { return callbacks.serialize; }
	aggregate_deserialize_t GetDeserializeCallback() const { return callbacks.deserialize; }
	// clang-format on

protected:
	AggregateFunctionCallbacks callbacks;
	AggregateFunctionProperties properties;

	//! Additional function info, passed to the bind
	shared_ptr<AggregateFunctionInfo> function_info;

public:
	// clang-format off
	FunctionStability GetStability() const { return properties.stability; }
	void SetStability(FunctionStability stability_p) { properties.stability = stability_p; }
	FunctionNullHandling GetNullHandling() const { return properties.null_handling; }
	void SetNullHandling(FunctionNullHandling null_handling_p) { properties.null_handling = null_handling_p; }
	FunctionErrors GetErrorMode() const { return properties.errors; }
	void SetErrorMode(FunctionErrors errors_p) { properties.errors = errors_p; }
	FunctionCollationHandling GetCollationHandling() const { return properties.collation_handling; }
	void SetCollationHandling(FunctionCollationHandling collation_handling_p) { properties.collation_handling = collation_handling_p; }

	//! Set this functions error-mode as fallible (can throw runtime errors)
	void SetFallible() { properties.errors = FunctionErrors::CAN_THROW_RUNTIME_ERROR; }
	//! Set this functions stability as volatile (can not be cached per row)
	void SetVolatile() { properties.stability = FunctionStability::VOLATILE; }
	// clang-format on

public:
	bool HasExtraFunctionInfo() const {
		return function_info != nullptr;
	}

	AggregateFunctionInfo &GetExtraFunctionInfo() const {
		D_ASSERT(function_info.get());
		return *function_info;
	}

	void SetExtraFunctionInfo(shared_ptr<AggregateFunctionInfo> info) {
		function_info = std::move(info);
	}

	template <class T, class... ARGS>
	void SetExtraFunctionInfo(ARGS &&... args) {
		function_info = make_shared_ptr<T>(std::forward<ARGS>(args)...);
	}

	AggregateOrderDependent GetOrderDependent() const {
		return properties.order_dependent;
	}
	void SetOrderDependent(AggregateOrderDependent value) {
		properties.order_dependent = value;
	}
	AggregateDistinctDependent GetDistinctDependent() const {
		return properties.distinct_dependent;
	}
	void SetDistinctDependent(AggregateDistinctDependent value) {
		properties.distinct_dependent = value;
	}

	bool HasGetStateTypeCallback() const {
		return callbacks.get_state_type != nullptr;
	}
	aggregate_get_state_type_t GetStateTypeCallback() const {
		return callbacks.get_state_type;
	}

	AggregateFunction &SetStructStateExport(aggregate_get_state_type_t get_state_type_callback) {
		callbacks.get_state_type = get_state_type_callback;
		return *this;
	}

	LogicalType GetStateType() const {
		D_ASSERT(callbacks.get_state_type);
		const auto result = callbacks.get_state_type(*this);
		// The underlying type of the AggregateState should be a struct
		D_ASSERT(result.id() == LogicalTypeId::STRUCT);
		return result;
	}

public:
	bool operator==(const AggregateFunction &rhs) const {
		return callbacks == rhs.callbacks;
	}
	bool operator!=(const AggregateFunction &rhs) const {
		return !(*this == rhs);
	}

	bool CanAggregate() const {
		return callbacks.update || callbacks.combine || callbacks.finalize;
	}
	bool CanWindow() const {
		return callbacks.window;
	}

public:
	template <class STATE, class RESULT_TYPE, class OP>
	static AggregateFunction NullaryAggregate(LogicalType return_type) {
		return AggregateFunction(
		    {}, return_type, AggregateFunction::StateSize<STATE>, AggregateFunction::StateInitialize<STATE, OP>,
		    AggregateFunction::NullaryScatterUpdate<STATE, OP>, AggregateFunction::StateCombine<STATE, OP>,
		    AggregateFunction::StateFinalize<STATE, RESULT_TYPE, OP>, AggregateFunction::NullaryUpdate<STATE, OP>);
	}

	template <class STATE, class INPUT_TYPE, class RESULT_TYPE, class OP,
	          AggregateDestructorType destructor_type = AggregateDestructorType::STANDARD>
	static AggregateFunction
	UnaryAggregate(const LogicalType &input_type, LogicalType return_type,
	               FunctionNullHandling null_handling = FunctionNullHandling::DEFAULT_NULL_HANDLING) {
		return AggregateFunction({input_type}, return_type, AggregateFunction::StateSize<STATE>,
		                         AggregateFunction::StateInitialize<STATE, OP, destructor_type>,
		                         AggregateFunction::UnaryScatterUpdate<STATE, INPUT_TYPE, OP>,
		                         AggregateFunction::StateCombine<STATE, OP>,
		                         AggregateFunction::StateFinalize<STATE, RESULT_TYPE, OP>, null_handling,
		                         AggregateFunction::UnaryUpdate<STATE, INPUT_TYPE, OP>);
	}

	template <class STATE, class INPUT_TYPE, class RESULT_TYPE, class OP,
	          AggregateDestructorType destructor_type = AggregateDestructorType::STANDARD>
	static AggregateFunction UnaryAggregateDestructor(LogicalType input_type, LogicalType return_type) {
		auto aggregate = UnaryAggregate<STATE, INPUT_TYPE, RESULT_TYPE, OP, destructor_type>(input_type, return_type);
		aggregate.callbacks.destructor = AggregateFunction::StateDestroy<STATE, OP>;
		return aggregate;
	}

	template <class STATE, class A_TYPE, class B_TYPE, class RESULT_TYPE, class OP,
	          AggregateDestructorType destructor_type = AggregateDestructorType::STANDARD>
	static AggregateFunction BinaryAggregate(const LogicalType &a_type, const LogicalType &b_type,
	                                         LogicalType return_type) {
		return AggregateFunction({a_type, b_type}, return_type, AggregateFunction::StateSize<STATE>,
		                         AggregateFunction::StateInitialize<STATE, OP, destructor_type>,
		                         AggregateFunction::BinaryScatterUpdate<STATE, A_TYPE, B_TYPE, OP>,
		                         AggregateFunction::StateCombine<STATE, OP>,
		                         AggregateFunction::StateFinalize<STATE, RESULT_TYPE, OP>,
		                         AggregateFunction::BinaryUpdate<STATE, A_TYPE, B_TYPE, OP>);
	}

public:
	template <class STATE>
	static idx_t StateSize(const AggregateFunction &) {
		return sizeof(STATE);
	}

	template <class STATE, class OP, AggregateDestructorType destructor_type = AggregateDestructorType::STANDARD>
	static void StateInitialize(const AggregateFunction &, data_ptr_t state) {
		// FIXME: we should remove the "destructor_type" option in the future
#if !defined(__GNUC__) || (__GNUC__ >= 5)
		static_assert(std::is_trivially_move_constructible<STATE>::value ||
		                  destructor_type == AggregateDestructorType::LEGACY,
		              "Aggregate state must be trivially move constructible");
#endif
		OP::Initialize(*reinterpret_cast<STATE *>(state));
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
	static void StateVoidFinalize(Vector &states, AggregateInputData &aggr_input_data, Vector &result, idx_t count,
	                              idx_t offset) {
		AggregateExecutor::VoidFinalize<STATE, OP>(states, aggr_input_data, result, count, offset);
	}

	template <class STATE, class OP>
	static void StateDestroy(Vector &states, AggregateInputData &aggr_input_data, idx_t count) {
		AggregateExecutor::Destroy<STATE, OP>(states, aggr_input_data, count);
	}
};

class BoundAggregateFunction : public AggregateFunction {
public:
	BoundAggregateFunction(const AggregateFunction &function)
	    : AggregateFunction(function) { // NOLINT: allow implicit conversion
	}
};

} // namespace duckdb
