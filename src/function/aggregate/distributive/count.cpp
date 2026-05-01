#include "duckdb/common/clustered_aggr.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/function/aggregate/distributive_function_utils.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"

namespace duckdb {

namespace {
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
		D_ASSERT(partition.column_ids.empty());

		auto data = FlatVector::GetDataMutable<RESULT_TYPE>(result);
		RESULT_TYPE total = 0;
		for (const auto &frame : frames) {
			const auto begin = frame.start;
			const auto end = frame.end;

			// Slice to any filtered rows
			if (partition.filter_mask.CannotHaveNull()) {
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
		state += UnsafeNumericCast<STATE>(count);
	}

	static bool IgnoreNull() {
		return true;
	}

	static inline void CountFlatLoop(STATE **__restrict states, const ValidityMask &mask, idx_t count) {
		if (mask.CanHaveNull()) {
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
	using STATE_PTR = STATE *const *;

	static inline void CountScatterLoop(STATE_PTR __restrict states, const SelectionVector &isel,
	                                    const SelectionVector &ssel, ValidityMask &mask, idx_t count) {
		if (mask.CanHaveNull()) {
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

	template <class INDEXER>
	static void CountClusteredRuns(const ClusteredAggr &cs, const ValidityMask &validity, INDEXER indexer) {
		const bool all_valid = !validity.CanHaveNull();
		idx_t pos = 0;
		for (idx_t r = 0; r < cs.n_group_runs; r++) {
			auto &state = *reinterpret_cast<STATE *>(cs.group_runs[r].state);
			const auto *run_sel = cs.group_runs[r].sel;
			const auto run_count = cs.group_runs[r].count;
			if (all_valid) {
				state += UnsafeNumericCast<STATE>(run_count);
			} else {
				for (idx_t k = 0; k < run_count; k++) {
					state += validity.RowIsValidUnsafe(indexer(run_sel, pos, k));
				}
			}
			pos += run_count;
		}
	}

	static void CountScatterClusteredGeneric(Vector &input, const ClusteredAggr &cs, idx_t count) {
		if (input.GetVectorType() == VectorType::FLAT_VECTOR) {
			auto &validity = FlatVector::Validity(input);
			CountClusteredRuns(cs, validity,
			                   [&](const sel_t *run_sel, idx_t, idx_t k) { return run_sel ? run_sel[k] : k; });
			return;
		}
		UnifiedVectorFormat idata;
		input.ToUnifiedFormat(count, idata);
		CountClusteredRuns(cs, idata.validity, [&](const sel_t *run_sel, idx_t, idx_t k) {
			return idata.sel->get_index(run_sel ? run_sel[k] : k);
		});
	}

	template <bool SIMPLE_DICT>
	static void CountClusteredDictionary(Vector &input, const ClusteredAggr &clustered, idx_t count,
	                                     const sel_t *cluster_iter = nullptr) {
		UnifiedVectorFormat idata;
		input.ToUnifiedFormat(count, idata);
		if constexpr (SIMPLE_DICT) {
			CountClusteredRuns(clustered, idata.validity,
			                   [&](const sel_t *, idx_t pos, idx_t k) { return cluster_iter[pos + k]; });
		} else {
			CountClusteredRuns(clustered, idata.validity, [&](const sel_t *run_sel, idx_t, idx_t k) {
				return idata.sel->get_index(run_sel ? run_sel[k] : k);
			});
		}
	}

	static void CountClusterUpdate(Vector inputs[], AggregateInputData &, idx_t input_count,
	                               const ClusteredAggr &clustered, idx_t count) {
		D_ASSERT(input_count == 1);
		if (inputs[0].GetVectorType() == VectorType::CONSTANT_VECTOR) {
			if (ConstantVector::IsNull(inputs[0])) {
				return;
			}
			for (idx_t r = 0; r < clustered.n_group_runs; r++) {
				auto &state = *reinterpret_cast<STATE *>(clustered.group_runs[r].state);
				state += UnsafeNumericCast<STATE>(clustered.group_runs[r].count);
			}
			return;
		}
		auto *cluster_iter = clustered.ClusterIter(inputs[0], count);
		if (cluster_iter) {
			CountClusteredDictionary<true>(inputs[0], clustered, count, cluster_iter);
			return;
		}
		CountScatterClusteredGeneric(inputs[0], clustered, count);
	}

	static void CountScatter(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count, Vector &states,
	                         idx_t count) {
		// COUNT(col) clustered fast path: add run counts when the input is all-valid,
		// otherwise count validity bits over each run. For simple DICT input, ClusterIter
		// pre-composes the dict sel once for the whole chunk.
		if (aggr_input_data.clustered) {
			auto &cs = *aggr_input_data.clustered;
			auto *cluster_iter = cs.ClusterIter(inputs[0], count);
			if (cluster_iter) {
				CountClusteredDictionary<true>(inputs[0], cs, count, cluster_iter);
				return;
			}
			CountScatterClusteredGeneric(inputs[0], cs, count);
			return;
		}
		auto &input = inputs[0];
		if (input.GetVectorType() == VectorType::FLAT_VECTOR && states.GetVectorType() == VectorType::FLAT_VECTOR) {
			auto sdata = FlatVector::GetDataMutable<STATE *>(states);
			CountFlatLoop(sdata, FlatVector::ValidityMutable(input), count);
		} else {
			UnifiedVectorFormat idata, sdata;
			input.ToUnifiedFormat(count, idata);
			states.ToUnifiedFormat(count, sdata);
			CountScatterLoop(UnifiedVectorFormat::GetData<STATE *>(sdata), *idata.sel, *sdata.sel, idata.validity,
			                 count);
		}
	}

	static inline void CountFlatUpdateLoop(STATE &result, const ValidityMask &mask, idx_t count) {
		idx_t base_idx = 0;
		auto entry_count = ValidityMask::EntryCount(count);
		for (idx_t entry_idx = 0; entry_idx < entry_count; entry_idx++) {
			auto validity_entry = mask.GetValidityEntry(entry_idx);
			idx_t next = MinValue<idx_t>(base_idx + ValidityMask::BITS_PER_VALUE, count);
			if (ValidityMask::AllValid(validity_entry)) {
				// all valid
				result += UnsafeNumericCast<STATE>(next - base_idx);
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
};

LogicalType GetCountStateType(const AggregateFunction &function) {
	child_list_t<LogicalType> children;
	children.emplace_back("count", LogicalType::BIGINT);
	return LogicalType::STRUCT(std::move(children));
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

} // namespace

AggregateFunction CountFunctionBase::GetFunction() {
	AggregateFunction fun({LogicalType(LogicalTypeId::ANY)}, LogicalType::BIGINT, AggregateFunction::StateSize<int64_t>,
	                      AggregateFunction::StateInitialize<int64_t, CountFunction>, CountFunction::CountScatter,
	                      AggregateFunction::StateCombine<int64_t, CountFunction>,
	                      AggregateFunction::StateFinalize<int64_t, int64_t, CountFunction>,
	                      FunctionNullHandling::SPECIAL_HANDLING, CountFunction::CountClusterUpdate);
	fun.name = "count";
	fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
	fun.SetStructStateExport(GetCountStateType);
	fun.SetStatisticsCallback(CountPropagateStats);
	return fun;
}

AggregateFunction CountStarFun::GetFunction() {
	auto fun = AggregateFunction::NullaryAggregate<int64_t, int64_t, CountStarFunction>(LogicalType::BIGINT);
	fun.name = "count_star";
	fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	fun.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
	fun.SetWindowCallback(CountStarFunction::Window<int64_t>);
	fun.SetStructStateExport(GetCountStateType);
	return fun;
}

AggregateFunctionSet CountFun::GetFunctions() {
	AggregateFunction count_function = CountFunctionBase::GetFunction();
	AggregateFunctionSet count("count");
	count.AddFunction(count_function);
	// the count function can also be called without arguments
	count.AddFunction(CountStarFun::GetFunction());
	return count;
}

} // namespace duckdb
