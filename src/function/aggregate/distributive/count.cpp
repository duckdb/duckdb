#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"

namespace duckdb {

struct BaseCountFunction {
	template <class STATE>
	static void Initialize(STATE &state) {
		state = 0;
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		target += source;
	}

	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		target = state;
	}
};

struct CountStarFunction : public BaseCountFunction {
	template <class STATE, class OP>
	static void Operation(STATE &state, AggregateInputData &, idx_t idx) {
		state += 1;
	}

	template <class STATE, class OP>
	static void ConstantOperation(STATE &state, AggregateInputData &, idx_t count) {
		state += count;
	}

	template <typename RESULT_TYPE>
	static void Window(AggregateInputData &aggr_input_data, const WindowPartitionInput &partition, const_data_ptr_t,
	                   data_ptr_t l_state, const SubFrames &frames, Vector &result, idx_t rid) {
		D_ASSERT(partition.input_count == 0);

		auto data = FlatVector::GetData<RESULT_TYPE>(result);
		RESULT_TYPE total = 0;
		for (const auto &frame : frames) {
			const auto begin = frame.start;
			const auto end = frame.end;

			// Slice to any filtered rows
			if (partition.filter_mask.AllValid()) {
				total += end - begin;
				continue;
			}
			for (auto i = begin; i < end; ++i) {
				total += partition.filter_mask.RowIsValid(i);
			}
		}
		data[rid] = total;
	}
};

struct CountFunction : public BaseCountFunction {
	using STATE = int64_t;

	static void Operation(STATE &state) {
		state += 1;
	}

	static void ConstantOperation(STATE &state, idx_t count) {
		state += count;
	}

	static bool IgnoreNull() {
		return true;
	}

	static inline void CountFlatLoop(STATE **__restrict states, ValidityMask &mask, idx_t count) {
		if (!mask.AllValid()) {
			idx_t base_idx = 0;
			auto entry_count = ValidityMask::EntryCount(count);
			for (idx_t entry_idx = 0; entry_idx < entry_count; entry_idx++) {
				auto validity_entry = mask.GetValidityEntry(entry_idx);
				idx_t next = MinValue<idx_t>(base_idx + ValidityMask::BITS_PER_VALUE, count);
				if (ValidityMask::AllValid(validity_entry)) {
					// all valid: perform operation
					for (; base_idx < next; base_idx++) {
						CountFunction::Operation(*states[base_idx]);
					}
				} else if (ValidityMask::NoneValid(validity_entry)) {
					// nothing valid: skip all
					base_idx = next;
					continue;
				} else {
					// partially valid: need to check individual elements for validity
					idx_t start = base_idx;
					for (; base_idx < next; base_idx++) {
						if (ValidityMask::RowIsValid(validity_entry, base_idx - start)) {
							CountFunction::Operation(*states[base_idx]);
						}
					}
				}
			}
		} else {
			for (idx_t i = 0; i < count; i++) {
				CountFunction::Operation(*states[i]);
			}
		}
	}

	static inline void CountScatterLoop(STATE **__restrict states, const SelectionVector &isel,
	                                    const SelectionVector &ssel, ValidityMask &mask, idx_t count) {
		if (!mask.AllValid()) {
			// potential NULL values
			for (idx_t i = 0; i < count; i++) {
				auto idx = isel.get_index(i);
				auto sidx = ssel.get_index(i);
				if (mask.RowIsValid(idx)) {
					CountFunction::Operation(*states[sidx]);
				}
			}
		} else {
			// quick path: no NULL values
			for (idx_t i = 0; i < count; i++) {
				auto sidx = ssel.get_index(i);
				CountFunction::Operation(*states[sidx]);
			}
		}
	}

	static void CountScatter(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count, Vector &states,
	                         idx_t count) {
		auto &input = inputs[0];
		if (input.GetVectorType() == VectorType::FLAT_VECTOR && states.GetVectorType() == VectorType::FLAT_VECTOR) {
			auto sdata = FlatVector::GetData<STATE *>(states);
			CountFlatLoop(sdata, FlatVector::Validity(input), count);
		} else {
			UnifiedVectorFormat idata, sdata;
			input.ToUnifiedFormat(count, idata);
			states.ToUnifiedFormat(count, sdata);
			CountScatterLoop(reinterpret_cast<STATE **>(sdata.data), *idata.sel, *sdata.sel, idata.validity, count);
		}
	}

	static inline void CountFlatUpdateLoop(STATE &result, ValidityMask &mask, idx_t count) {
		idx_t base_idx = 0;
		auto entry_count = ValidityMask::EntryCount(count);
		for (idx_t entry_idx = 0; entry_idx < entry_count; entry_idx++) {
			auto validity_entry = mask.GetValidityEntry(entry_idx);
			idx_t next = MinValue<idx_t>(base_idx + ValidityMask::BITS_PER_VALUE, count);
			if (ValidityMask::AllValid(validity_entry)) {
				// all valid
				result += next - base_idx;
				base_idx = next;
			} else if (ValidityMask::NoneValid(validity_entry)) {
				// nothing valid: skip all
				base_idx = next;
				continue;
			} else {
				// partially valid: need to check individual elements for validity
				idx_t start = base_idx;
				for (; base_idx < next; base_idx++) {
					if (ValidityMask::RowIsValid(validity_entry, base_idx - start)) {
						result++;
					}
				}
			}
		}
	}

	static inline void CountUpdateLoop(STATE &result, ValidityMask &mask, idx_t count,
	                                   const SelectionVector &sel_vector) {
		if (mask.AllValid()) {
			// no NULL values
			result += count;
			return;
		}
		for (idx_t i = 0; i < count; i++) {
			auto idx = sel_vector.get_index(i);
			if (mask.RowIsValid(idx)) {
				result++;
			}
		}
	}

	static void CountUpdate(Vector inputs[], AggregateInputData &, idx_t input_count, data_ptr_t state_p, idx_t count) {
		auto &input = inputs[0];
		auto &result = *reinterpret_cast<STATE *>(state_p);
		switch (input.GetVectorType()) {
		case VectorType::CONSTANT_VECTOR: {
			if (!ConstantVector::IsNull(input)) {
				// if the constant is not null increment the state
				result += count;
			}
			break;
		}
		case VectorType::FLAT_VECTOR: {
			CountFlatUpdateLoop(result, FlatVector::Validity(input), count);
			break;
		}
		case VectorType::SEQUENCE_VECTOR: {
			// sequence vectors cannot have NULL values
			result += count;
			break;
		}
		default: {
			UnifiedVectorFormat idata;
			input.ToUnifiedFormat(count, idata);
			CountUpdateLoop(result, idata.validity, count, *idata.sel);
			break;
		}
		}
	}
};

AggregateFunction CountFun::GetFunction() {
	AggregateFunction fun({LogicalType(LogicalTypeId::ANY)}, LogicalType::BIGINT, AggregateFunction::StateSize<int64_t>,
	                      AggregateFunction::StateInitialize<int64_t, CountFunction>, CountFunction::CountScatter,
	                      AggregateFunction::StateCombine<int64_t, CountFunction>,
	                      AggregateFunction::StateFinalize<int64_t, int64_t, CountFunction>,
	                      FunctionNullHandling::SPECIAL_HANDLING, CountFunction::CountUpdate);
	fun.name = "count";
	fun.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
	return fun;
}

AggregateFunction CountStarFun::GetFunction() {
	auto fun = AggregateFunction::NullaryAggregate<int64_t, int64_t, CountStarFunction>(LogicalType::BIGINT);
	fun.name = "count_star";
	fun.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	fun.order_dependent = AggregateOrderDependent::NOT_ORDER_DEPENDENT;
	fun.window = CountStarFunction::Window<int64_t>;
	return fun;
}

unique_ptr<BaseStatistics> CountPropagateStats(ClientContext &context, BoundAggregateExpression &expr,
                                               AggregateStatisticsInput &input) {
	if (!expr.IsDistinct() && !input.child_stats[0].CanHaveNull()) {
		// count on a column without null values: use count star
		expr.function = CountStarFun::GetFunction();
		expr.function.name = "count_star";
		expr.children.clear();
	}
	return nullptr;
}

void CountFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunction count_function = CountFun::GetFunction();
	count_function.statistics = CountPropagateStats;
	AggregateFunctionSet count("count");
	count.AddFunction(count_function);
	// the count function can also be called without arguments
	count_function = CountStarFun::GetFunction();
	count.AddFunction(count_function);
	set.AddFunction(count);
}

void CountStarFun::RegisterFunction(BuiltinFunctions &set) {
	AggregateFunctionSet count("count_star");
	count.AddFunction(CountStarFun::GetFunction());
	set.AddFunction(count);
}

} // namespace duckdb
