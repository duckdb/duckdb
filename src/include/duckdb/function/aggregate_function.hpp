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
	BindAggregateFunctionInput(ClientContext &context_p, AggregateFunction &bound_function_p,
	                           vector<unique_ptr<Expression>> &arguments_p)
	    : context(context_p), bound_function(bound_function_p), arguments(arguments_p) {
	}

	ClientContext &GetClientContext() const {
		return context;
	}
	AggregateFunction &GetBoundFunction() const {
		return bound_function;
	}
	vector<unique_ptr<Expression>> &GetArguments() const {
		return arguments;
	}

private:
	ClientContext &context;
	AggregateFunction &bound_function;
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

//! The type used for updating a clustered set of aggregate states.
typedef void (*aggregate_cluster_update_t)(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count,
                                           const ClusteredAggr &clustered, idx_t count);

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
                                      const AggregateFunction &function);
typedef unique_ptr<FunctionData> (*aggregate_deserialize_t)(Deserializer &deserializer, AggregateFunction &function);

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

class AggregateFunction : public BaseScalarFunction { // NOLINT: work-around bug in clang-tidy
public:
	AggregateFunction(const string &name, const vector<LogicalType> &arguments, const LogicalType &return_type,
	                  aggregate_size_t state_size, aggregate_initialize_t initialize, aggregate_update_t update,
	                  aggregate_combine_t combine, aggregate_finalize_t finalize,
	                  FunctionNullHandling null_handling = FunctionNullHandling::DEFAULT_NULL_HANDLING,
	                  aggregate_cluster_update_t cluster_update_p = nullptr, bind_aggregate_function_t bind = nullptr,
	                  aggregate_destructor_t destructor = nullptr, aggregate_statistics_t statistics = nullptr,
	                  aggregate_window_t window = nullptr, aggregate_serialize_t serialize = nullptr,
	                  aggregate_deserialize_t deserialize = nullptr)
	    : BaseScalarFunction(name, arguments, return_type, FunctionStability::CONSISTENT,
	                         LogicalType(LogicalTypeId::INVALID), null_handling),
	      state_size(state_size), initialize(initialize), update(update), combine(combine), finalize(finalize),
	      cluster_update(cluster_update_p), window(window), bind(bind), destructor(destructor), statistics(statistics),
	      serialize(serialize), deserialize(deserialize), order_dependent(AggregateOrderDependent::ORDER_DEPENDENT),
	      distinct_dependent(AggregateDistinctDependent::DISTINCT_DEPENDENT) {
	}

	AggregateFunction(const string &name, const vector<LogicalType> &arguments, const LogicalType &return_type,
	                  aggregate_size_t state_size, aggregate_initialize_t initialize, aggregate_update_t update,
	                  aggregate_combine_t combine, aggregate_finalize_t finalize, std::nullptr_t,
	                  aggregate_destructor_t destructor = nullptr, aggregate_statistics_t statistics = nullptr,
	                  aggregate_window_t window = nullptr, aggregate_serialize_t serialize = nullptr,
	                  aggregate_deserialize_t deserialize = nullptr)
	    : AggregateFunction(name, arguments, return_type, state_size, initialize, update, combine, finalize,
	                        FunctionNullHandling::DEFAULT_NULL_HANDLING,
	                        static_cast<aggregate_cluster_update_t>(nullptr), nullptr, destructor, statistics, window,
	                        serialize, deserialize) {
	}

	AggregateFunction(const string &name, const vector<LogicalType> &arguments, const LogicalType &return_type,
	                  aggregate_size_t state_size, aggregate_initialize_t initialize, aggregate_update_t update,
	                  aggregate_combine_t combine, aggregate_finalize_t finalize, std::nullptr_t,
	                  bind_aggregate_function_t bind, aggregate_destructor_t destructor = nullptr,
	                  aggregate_statistics_t statistics = nullptr, aggregate_window_t window = nullptr,
	                  aggregate_serialize_t serialize = nullptr, aggregate_deserialize_t deserialize = nullptr)
	    : AggregateFunction(name, arguments, return_type, state_size, initialize, update, combine, finalize,
	                        FunctionNullHandling::DEFAULT_NULL_HANDLING,
	                        static_cast<aggregate_cluster_update_t>(nullptr), bind, destructor, statistics, window,
	                        serialize, deserialize) {
	}

	AggregateFunction(const string &name, const vector<LogicalType> &arguments, const LogicalType &return_type,
	                  aggregate_size_t state_size, aggregate_initialize_t initialize, aggregate_update_t update,
	                  aggregate_combine_t combine, aggregate_finalize_t finalize, FunctionNullHandling null_handling,
	                  std::nullptr_t, bind_aggregate_function_t bind, aggregate_destructor_t destructor = nullptr,
	                  aggregate_statistics_t statistics = nullptr, aggregate_window_t window = nullptr,
	                  aggregate_serialize_t serialize = nullptr, aggregate_deserialize_t deserialize = nullptr)
	    : AggregateFunction(name, arguments, return_type, state_size, initialize, update, combine, finalize,
	                        null_handling, static_cast<aggregate_cluster_update_t>(nullptr), bind, destructor,
	                        statistics, window, serialize, deserialize) {
	}

	AggregateFunction(const vector<LogicalType> &arguments, const LogicalType &return_type, aggregate_size_t state_size,
	                  aggregate_initialize_t initialize, aggregate_update_t update, aggregate_combine_t combine,
	                  aggregate_finalize_t finalize,
	                  FunctionNullHandling null_handling = FunctionNullHandling::DEFAULT_NULL_HANDLING,
	                  aggregate_cluster_update_t cluster_update_p = nullptr, bind_aggregate_function_t bind = nullptr,
	                  aggregate_destructor_t destructor = nullptr, aggregate_statistics_t statistics = nullptr,
	                  aggregate_window_t window = nullptr, aggregate_serialize_t serialize = nullptr,
	                  aggregate_deserialize_t deserialize = nullptr)
	    : AggregateFunction(string(), arguments, return_type, state_size, initialize, update, combine, finalize,
	                        null_handling, cluster_update_p, bind, destructor, statistics, window, serialize,
	                        deserialize) {
	}

	AggregateFunction(const vector<LogicalType> &arguments, const LogicalType &return_type, aggregate_size_t state_size,
	                  aggregate_initialize_t initialize, aggregate_update_t update, aggregate_combine_t combine,
	                  aggregate_finalize_t finalize, std::nullptr_t, aggregate_destructor_t destructor = nullptr,
	                  aggregate_statistics_t statistics = nullptr, aggregate_window_t window = nullptr,
	                  aggregate_serialize_t serialize = nullptr, aggregate_deserialize_t deserialize = nullptr)
	    : AggregateFunction(string(), arguments, return_type, state_size, initialize, update, combine, finalize,
	                        FunctionNullHandling::DEFAULT_NULL_HANDLING,
	                        static_cast<aggregate_cluster_update_t>(nullptr), nullptr, destructor, statistics, window,
	                        serialize, deserialize) {
	}

	AggregateFunction(const vector<LogicalType> &arguments, const LogicalType &return_type, aggregate_size_t state_size,
	                  aggregate_initialize_t initialize, aggregate_update_t update, aggregate_combine_t combine,
	                  aggregate_finalize_t finalize, std::nullptr_t, bind_aggregate_function_t bind,
	                  aggregate_destructor_t destructor = nullptr, aggregate_statistics_t statistics = nullptr,
	                  aggregate_window_t window = nullptr, aggregate_serialize_t serialize = nullptr,
	                  aggregate_deserialize_t deserialize = nullptr)
	    : AggregateFunction(string(), arguments, return_type, state_size, initialize, update, combine, finalize,
	                        FunctionNullHandling::DEFAULT_NULL_HANDLING,
	                        static_cast<aggregate_cluster_update_t>(nullptr), bind, destructor, statistics, window,
	                        serialize, deserialize) {
	}

	AggregateFunction(const vector<LogicalType> &arguments, const LogicalType &return_type, aggregate_size_t state_size,
	                  aggregate_initialize_t initialize, aggregate_update_t update, aggregate_combine_t combine,
	                  aggregate_finalize_t finalize, FunctionNullHandling null_handling, std::nullptr_t,
	                  bind_aggregate_function_t bind, aggregate_destructor_t destructor = nullptr,
	                  aggregate_statistics_t statistics = nullptr, aggregate_window_t window = nullptr,
	                  aggregate_serialize_t serialize = nullptr, aggregate_deserialize_t deserialize = nullptr)
	    : AggregateFunction(string(), arguments, return_type, state_size, initialize, update, combine, finalize,
	                        null_handling, static_cast<aggregate_cluster_update_t>(nullptr), bind, destructor,
	                        statistics, window, serialize, deserialize) {
	}

	// Window constructor
	AggregateFunction(const vector<LogicalType> &arguments, const LogicalType &return_type, aggregate_size_t state_size,
	                  aggregate_initialize_t initialize, aggregate_wininit_t window_init, aggregate_window_t window,
	                  bind_aggregate_function_t bind = nullptr, aggregate_destructor_t destructor = nullptr,
	                  aggregate_statistics_t statistics = nullptr, aggregate_serialize_t serialize = nullptr,
	                  aggregate_deserialize_t deserialize = nullptr)
	    : BaseScalarFunction(name, arguments, return_type, FunctionStability::CONSISTENT,
	                         LogicalType(LogicalTypeId::INVALID)),
	      state_size(state_size), initialize(initialize), update(nullptr), combine(nullptr), finalize(nullptr),
	      cluster_update(nullptr), window(window), window_init(window_init), bind(bind), destructor(destructor),
	      statistics(statistics), serialize(serialize), deserialize(deserialize),
	      order_dependent(AggregateOrderDependent::ORDER_DEPENDENT),
	      distinct_dependent(AggregateDistinctDependent::DISTINCT_DEPENDENT) {
	}

	// clang-format off
	bool HasBindCallback() const { return bind != nullptr; }
	bind_aggregate_function_t GetBindCallback() const { return bind; }
	void SetBindCallback(bind_aggregate_function_t callback) { bind = callback; }
	unique_ptr<FunctionData> Bind(BindAggregateFunctionInput &bind_input) { return GetBindCallback()(bind_input); }
	unique_ptr<FunctionData> Bind(ClientContext &context, vector<unique_ptr<Expression>> &arguments) {
		BindAggregateFunctionInput bind_input(context, *this, arguments);
		return Bind(bind_input);
	}

	bool HasStateInitCallback() const { return initialize != nullptr; }
	aggregate_initialize_t GetStateInitCallback() const { return initialize; }
	void SetStateInitCallback(aggregate_initialize_t callback) { initialize = callback; }

	bool HasStateSizeCallback() const { return state_size != nullptr; }
	aggregate_size_t GetStateSizeCallback() const { return state_size; }
	void SetStateSizeCallback(aggregate_size_t callback) { state_size = callback; }

	bool HasStateDestructorCallback() const { return destructor != nullptr; }
	aggregate_destructor_t GetStateDestructorCallback() const { return destructor; }
	void SetStateDestructorCallback(aggregate_destructor_t callback) { destructor = callback; }

	bool HasStateUpdateCallback() const { return update != nullptr; }
	aggregate_update_t GetStateUpdateCallback() const { return update; }
	void SetStateUpdateCallback(aggregate_update_t callback) { update = callback; }

	aggregate_cluster_update_t GetStateClusterUpdateCallback() const { return cluster_update; }
	void SetStateClusterUpdateCallback(aggregate_cluster_update_t callback) { cluster_update = callback; }

	void SetStateCombineCallback(aggregate_combine_t callback) { combine = callback; }
	aggregate_combine_t GetStateCombineCallback() const { return combine; }
	bool HasStateCombineCallback() const { return combine != nullptr; }

	void SetStateFinalizeCallback(aggregate_finalize_t callback) { finalize = callback; }
	aggregate_finalize_t GetStateFinalizeCallback() const { return finalize; }
	bool HasStateFinalizeCallback() const { return finalize != nullptr; }

	bool HasWindowCallback() const { return window != nullptr; }
	aggregate_window_t GetWindowCallback() const { return window; }
	void SetWindowCallback(aggregate_window_t callback) { window = callback; }

	void SetWindowInitCallback(aggregate_wininit_t callback) { window_init = callback; }
	aggregate_wininit_t GetWindowInitCallback() const { return window_init; }
	bool HasWindowInitCallback() const { return window_init != nullptr; }

	//! Batched window callback — takes precedence over the per-row window
	//! callback when set. See aggregate_window_batch_t for semantics.
	bool HasWindowBatchCallback() const { return window_batch != nullptr; }
	aggregate_window_batch_t GetWindowBatchCallback() const { return window_batch; }
	void SetWindowBatchCallback(aggregate_window_batch_t callback) { window_batch = callback; }

	bool HasStatisticsCallback() const { return statistics != nullptr; }
	aggregate_statistics_t GetStatisticsCallback() const { return statistics; }
	void SetStatisticsCallback(aggregate_statistics_t callback) { statistics = callback; }

	bool HasSerializationCallbacks() const { return serialize != nullptr && deserialize != nullptr; }
	void SetSerializeCallback(aggregate_serialize_t callback) { serialize = callback; }
	void SetDeserializeCallback(aggregate_deserialize_t callback) { deserialize = callback; }
	aggregate_serialize_t GetSerializeCallback() const { return serialize; }
	aggregate_deserialize_t GetDeserializeCallback() const { return deserialize; }
	// clang-format on

protected:
	//! The hashed aggregate state sizing function
	aggregate_size_t state_size;
	//! The hashed aggregate state initialization function
	aggregate_initialize_t initialize;
	//! The hashed aggregate update state function (may be null, if window is set)
	aggregate_update_t update;
	//! The hashed aggregate combine states function (may be null, if window is set)
	aggregate_combine_t combine;
	//! The hashed aggregate finalization function (may be null, if window is set)
	aggregate_finalize_t finalize;
	//! The clustered aggregate update function (may be null)
	aggregate_cluster_update_t cluster_update;
	//! The windowed aggregate custom function (may be null)
	aggregate_window_t window;
	//! The windowed aggregate custom initialization function (may be null)
	aggregate_wininit_t window_init = nullptr;
	//! Batched windowed aggregate function (may be null; preferred when set)
	aggregate_window_batch_t window_batch = nullptr;

	//! The bind function (may be null)
	bind_aggregate_function_t bind;
	//! The destructor method (may be null)
	aggregate_destructor_t destructor;

	//! The statistics propagation function (may be null)
	aggregate_statistics_t statistics;

	aggregate_serialize_t serialize;
	aggregate_deserialize_t deserialize;

	//! Whether or not the aggregate is order dependent
	AggregateOrderDependent order_dependent;
	//! Whether or not the aggregate is affect by distinct modifiers
	AggregateDistinctDependent distinct_dependent;
	aggregate_get_state_type_t get_state_type = nullptr;

	//! Additional function info, passed to the bind
	shared_ptr<AggregateFunctionInfo> function_info;

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
		return order_dependent;
	}
	void SetOrderDependent(AggregateOrderDependent value) {
		order_dependent = value;
	}
	AggregateDistinctDependent GetDistinctDependent() const {
		return distinct_dependent;
	}
	void SetDistinctDependent(AggregateDistinctDependent value) {
		distinct_dependent = value;
	}

	bool HasGetStateTypeCallback() const {
		return get_state_type != nullptr;
	}
	aggregate_get_state_type_t GetStateTypeCallback() const {
		return get_state_type;
	}

	AggregateFunction &SetStructStateExport(aggregate_get_state_type_t get_state_type_callback) {
		get_state_type = get_state_type_callback;
		return *this;
	}

	LogicalType GetStateType() const {
		D_ASSERT(get_state_type);
		const auto result = get_state_type(*this);
		// The underlying type of the AggregateState should be a struct
		D_ASSERT(result.id() == LogicalTypeId::STRUCT);
		return result;
	}

public:
	bool operator==(const AggregateFunction &rhs) const {
		return state_size == rhs.state_size && initialize == rhs.initialize && update == rhs.update &&
		       combine == rhs.combine && finalize == rhs.finalize && window == rhs.window;
	}
	bool operator!=(const AggregateFunction &rhs) const {
		return !(*this == rhs);
	}

	bool CanAggregate() const {
		return update || combine || finalize;
	}
	bool CanWindow() const {
		return window;
	}

public:
	template <class...>
	using void_t_helper = void;

	template <class OP, class STATE, class = void>
	struct HasClusteredLocalState : std::false_type {};
	template <class OP, class STATE>
	struct HasClusteredLocalState<OP, STATE, void_t_helper<typename OP::template ClusteredLocalState<STATE>::Type>>
	    : std::true_type {};

	template <class OP, class = void>
	struct HasClusteredOperation : std::false_type {};
	template <class OP>
	struct HasClusteredOperation<OP, void_t_helper<decltype(&OP::template ClusteredOperation<int32_t, int32_t, OP>)>>
	    : std::true_type {};

	template <class STATE, class INPUT_TYPE, class OP>
	static aggregate_cluster_update_t UnaryClusterUpdateCallback() {
		if constexpr (HasClusteredLocalState<OP, STATE>::value || HasClusteredOperation<OP>::value) {
			return AggregateFunction::UnaryClusterUpdate<STATE, INPUT_TYPE, OP>;
		} else {
			return nullptr;
		}
	}

	template <class STATE, class RESULT_TYPE, class OP>
	static AggregateFunction NullaryAggregate(LogicalType return_type) {
		auto function = AggregateFunction(
		    string(), {}, return_type, AggregateFunction::StateSize<STATE>,
		    AggregateFunction::StateInitialize<STATE, OP>, AggregateFunction::NullaryScatterUpdate<STATE, OP>,
		    AggregateFunction::StateCombine<STATE, OP>, AggregateFunction::StateFinalize<STATE, RESULT_TYPE, OP>,
		    FunctionNullHandling::DEFAULT_NULL_HANDLING, AggregateFunction::NullaryClusterUpdate<STATE, OP>);
		return function;
	}

	template <class STATE, class INPUT_TYPE, class RESULT_TYPE, class OP,
	          AggregateDestructorType destructor_type = AggregateDestructorType::STANDARD>
	static AggregateFunction
	UnaryAggregate(const LogicalType &input_type, LogicalType return_type,
	               FunctionNullHandling null_handling = FunctionNullHandling::DEFAULT_NULL_HANDLING) {
		auto function = AggregateFunction(string(), {input_type}, return_type, AggregateFunction::StateSize<STATE>,
		                                  AggregateFunction::StateInitialize<STATE, OP, destructor_type>,
		                                  AggregateFunction::UnaryScatterUpdate<STATE, INPUT_TYPE, OP>,
		                                  AggregateFunction::StateCombine<STATE, OP>,
		                                  AggregateFunction::StateFinalize<STATE, RESULT_TYPE, OP>, null_handling,
		                                  UnaryClusterUpdateCallback<STATE, INPUT_TYPE, OP>());
		return function;
	}

	template <class STATE, class INPUT_TYPE, class RESULT_TYPE, class OP,
	          AggregateDestructorType destructor_type = AggregateDestructorType::STANDARD>
	static AggregateFunction UnaryAggregateDestructor(LogicalType input_type, LogicalType return_type) {
		auto aggregate = UnaryAggregate<STATE, INPUT_TYPE, RESULT_TYPE, OP, destructor_type>(input_type, return_type);
		aggregate.destructor = AggregateFunction::StateDestroy<STATE, OP>;
		return aggregate;
	}

	template <class STATE, class A_TYPE, class B_TYPE, class RESULT_TYPE, class OP,
	          AggregateDestructorType destructor_type = AggregateDestructorType::STANDARD>
	static AggregateFunction BinaryAggregate(const LogicalType &a_type, const LogicalType &b_type,
	                                         LogicalType return_type) {
		auto function = AggregateFunction({a_type, b_type}, return_type, AggregateFunction::StateSize<STATE>,
		                                  AggregateFunction::StateInitialize<STATE, OP, destructor_type>,
		                                  AggregateFunction::BinaryScatterUpdate<STATE, A_TYPE, B_TYPE, OP>,
		                                  AggregateFunction::StateCombine<STATE, OP>,
		                                  AggregateFunction::StateFinalize<STATE, RESULT_TYPE, OP>);
		return function;
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

	template <class STATE, class OP>
	static void NullaryClusterUpdate(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count,
	                                 const ClusteredAggr &clustered, idx_t count) {
		D_ASSERT(input_count == 0);
		AggregateExecutor::NullaryClusterUpdate<STATE, OP>(aggr_input_data, clustered, count);
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

	template <class STATE, class INPUT_TYPE, class OP>
	static void UnaryClusterUpdate(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count,
	                               const ClusteredAggr &clustered, idx_t count) {
		D_ASSERT(input_count == 1);
		AggregateExecutor::UnaryClusterUpdate<STATE, INPUT_TYPE, OP>(inputs[0], aggr_input_data, clustered, count);
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

} // namespace duckdb
