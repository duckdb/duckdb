#include "core_functions/aggregate/holistic_functions.hpp"
#include <algorithm>
#include <iterator>
#include <type_traits>
#include "vergesort.h"
#include "duckdb/planner/expression.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

namespace {

struct FallbackSort {
	template <class Iter>
	void operator()(Iter first, Iter last) const {
		std::sort(first, last);
	}
};

template <typename EVENT_T>
struct MaxIntersectionsState {
	using event_t = EVENT_T;
	vector<EVENT_T> events;

	MaxIntersectionsState() = default;

	MaxIntersectionsState(const MaxIntersectionsState &other) : events(other.events) {
	}

	MaxIntersectionsState &operator=(const MaxIntersectionsState &other) {
		if (this != &other) {
			events = other.events;
		}
		return *this;
	}

	MaxIntersectionsState(MaxIntersectionsState &&other) noexcept : events(std::move(other.events)) {
	}

	MaxIntersectionsState &operator=(MaxIntersectionsState &&other) noexcept {
		if (this != &other) {
			events = std::move(other.events);
		}
		return *this;
	}
};

// flag: 0 = start, 1 = end
template <typename EVENT_T, typename INPUT_T>
inline EVENT_T EncodeEvent(INPUT_T t, bool is_end) {
	const EVENT_T tt = static_cast<EVENT_T>(t);
	return (tt << 1) | static_cast<EVENT_T>(is_end ? 1 : 0);
}

template <typename EVENT_T>
struct MaxIntersectionsFunction {
	template <class STATE>
	static void Initialize(STATE &state) {
		new (&state) STATE();
	}

	template <class STATE>
	static void Destroy(STATE &state, AggregateInputData &aggr_input_data) {
		state.~STATE();
	}

	template <class A_TYPE, class B_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const A_TYPE &left, const B_TYPE &right, AggregateBinaryInput &input) {
		if (left < 0 || right < 0) {
			throw InvalidInputException("max_intersections: interval bounds cannot be negative");
		}
		if (left <= right) {
			const A_TYPE start_val = static_cast<A_TYPE>(left);
			const A_TYPE end_val = static_cast<A_TYPE>(right) + 1;
			state.events.emplace_back(EncodeEvent<EVENT_T, A_TYPE>(start_val, false));
			state.events.emplace_back(EncodeEvent<EVENT_T, A_TYPE>(end_val, true));
		}
	}

	template <class A_TYPE, class B_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const A_TYPE &left, const B_TYPE &right, AggregateBinaryInput &input,
	                              idx_t count) {
		if (left < 0 || right < 0) {
			throw InvalidInputException("max_intersections: interval bounds cannot be negative");
		}
		if (left <= right && count > 0) {
			const A_TYPE start_val = static_cast<A_TYPE>(left);
			const A_TYPE end_val = static_cast<A_TYPE>(right) + 1;
			state.events.reserve(state.events.size() + count * 2);
			for (idx_t i = 0; i < count; i++) {
				state.events.emplace_back(EncodeEvent<EVENT_T, A_TYPE>(start_val, false));
				state.events.emplace_back(EncodeEvent<EVENT_T, A_TYPE>(end_val, true));
			}
		}
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &aggr_input_data) {
		if (source.events.empty()) {
			return;
		}
		if (aggr_input_data.combine_type == AggregateCombineType::ALLOW_DESTRUCTIVE) {
			auto &mutable_source = const_cast<STATE &>(source);
			target.events.insert(target.events.end(), std::make_move_iterator(mutable_source.events.begin()),
			                     std::make_move_iterator(mutable_source.events.end()));
			mutable_source.events.clear();
		} else {
			target.events.insert(target.events.end(), source.events.begin(), source.events.end());
		}
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (state.events.empty()) {
			target = 0;
			return;
		}
		if (state.events.size() == 1) {
			target = 1;
			return;
		}

		duckdb_vergesort::vergesort(state.events.begin(), state.events.end(), std::less<EVENT_T>(), FallbackSort {});

		int64_t current_count = 0;
		int64_t max_count = 0;
		for (const auto ev : state.events) {
			// flag = ev & 1; 0 -> start, 1 -> end
			if ((ev & static_cast<EVENT_T>(1)) == static_cast<EVENT_T>(0)) {
				current_count++;
				max_count = MaxValue(current_count, max_count);
			} else {
				current_count--;
			}
		}
		target = max_count;
	}

	static bool IgnoreNull() {
		return true;
	}
};

} // namespace

template <typename INPUT_T, typename EVENT_T>
static AggregateFunction GetMaxIntersectionsTypedFunction(const LogicalType &type) {
	using STATE_T = MaxIntersectionsState<EVENT_T>;
	auto function =
	    AggregateFunction::BinaryAggregate<STATE_T, INPUT_T, INPUT_T, int64_t, MaxIntersectionsFunction<EVENT_T>,
	                                       AggregateDestructorType::LEGACY>(type, type, LogicalType::BIGINT);
	function.name = "max_intersections";
	function.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
	function.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	function.SetStateDestructorCallback(AggregateFunction::StateDestroy<STATE_T, MaxIntersectionsFunction<EVENT_T>>);
	return function;
}

static AggregateFunction MakeMaxIntersectionsTypedFunction(const LogicalType &arg_type) {
	switch (arg_type.id()) {
	case LogicalTypeId::SQLNULL:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::DATE:
		return GetMaxIntersectionsTypedFunction<int32_t, uint32_t>(arg_type);
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::BIGINT:
		return GetMaxIntersectionsTypedFunction<int64_t, uint64_t>(arg_type);
	default:
		throw NotImplementedException("Unimplemented argument type in max_intersections");
	}
}

static unique_ptr<FunctionData> BindMaxIntersections(ClientContext &context, AggregateFunction &function,
                                                     vector<unique_ptr<Expression>> &arguments) {
	// Choose implementation based on the first argument's type
	const auto &left_type = arguments[0]->return_type;
	function = MakeMaxIntersectionsTypedFunction(left_type);
	return nullptr;
}

AggregateFunction MaxIntersectionsFun::GetFunction() {
	// Stub function: actual implementation is selected during bind based on argument types
	AggregateFunction function({LogicalTypeId::ANY, LogicalTypeId::ANY}, LogicalTypeId::BIGINT, nullptr, nullptr,
	                           nullptr, nullptr, nullptr, nullptr, BindMaxIntersections);

	function.name = "max_intersections";
	function.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
	function.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);

	return function;
}

} // namespace duckdb
