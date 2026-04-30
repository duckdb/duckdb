//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector_operations/aggregate_executor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector/constant_vector.hpp"
#include "duckdb/common/vector/dictionary_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/aggregate_state.hpp"
#include <type_traits>

namespace duckdb {

// structs
struct AggregateInputData;

// The bounds of a window frame
struct FrameBounds {
	FrameBounds() : start(0), end(0) {};
	FrameBounds(idx_t start, idx_t end) : start(start), end(end) {};
	idx_t start = 0;
	idx_t end = 0;
};

// A set of window subframes for windowed EXCLUDE
using SubFrames = vector<FrameBounds>;

class AggregateExecutor {
private:
#ifndef DUCKDB_SMALLER_BINARY
	template <class STATE_TYPE, class OP>
	static inline void NullaryFlatLoop(STATE_TYPE **__restrict states, AggregateInputData &aggr_input_data,
	                                   idx_t count) {
		for (idx_t i = 0; i < count; i++) {
			OP::template Operation<STATE_TYPE, OP>(*states[i], aggr_input_data, i);
		}
	}
#endif

	template <class STATE_TYPE, class OP>
	static inline void NullaryScatterLoop(STATE_TYPE *__restrict const *__restrict const states,
	                                      AggregateInputData &aggr_input_data, const SelectionVector &ssel,
	                                      const idx_t count) {
		if (ssel.IsSet()) {
			for (idx_t i = 0; i < count; i++) {
				auto sidx = ssel.get_index_unsafe(i);
				OP::template Operation<STATE_TYPE, OP>(*states[sidx], aggr_input_data, sidx);
			}
		} else {
			for (idx_t i = 0; i < count; i++) {
				OP::template Operation<STATE_TYPE, OP>(*states[i], aggr_input_data, i);
			}
		}
	}

#ifndef DUCKDB_SMALLER_BINARY
	template <class STATE_TYPE, class INPUT_TYPE, class OP>
	static inline void UnaryFlatLoop(const INPUT_TYPE *__restrict idata, AggregateInputData &aggr_input_data,
	                                 STATE_TYPE **__restrict states, const ValidityMask &mask, idx_t count) {
		if (OP::IgnoreNull() && mask.CanHaveNull()) {
			AggregateUnaryInput input(aggr_input_data, mask);
			auto &base_idx = input.input_idx;
			base_idx = 0;
			auto entry_count = ValidityMask::EntryCount(count);
			for (idx_t entry_idx = 0; entry_idx < entry_count; entry_idx++) {
				auto validity_entry = mask.GetValidityEntry(entry_idx);
				idx_t next = MinValue<idx_t>(base_idx + ValidityMask::BITS_PER_VALUE, count);
				if (ValidityMask::AllValid(validity_entry)) {
					// all valid: perform operation
					for (; base_idx < next; base_idx++) {
						OP::template Operation<INPUT_TYPE, STATE_TYPE, OP>(*states[base_idx], idata[base_idx], input);
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
							OP::template Operation<INPUT_TYPE, STATE_TYPE, OP>(*states[base_idx], idata[base_idx],
							                                                   input);
						}
					}
				}
			}
		} else {
			AggregateUnaryInput input(aggr_input_data, mask);
			auto &i = input.input_idx;
			for (i = 0; i < count; i++) {
				OP::template Operation<INPUT_TYPE, STATE_TYPE, OP>(*states[i], idata[i], input);
			}
		}
	}
#endif

#ifndef DUCKDB_SMALLER_BINARY
	template <class STATE_TYPE, class INPUT_TYPE, class OP, bool HAS_ISEL, bool HAS_SSEL>
#else
	template <class STATE_TYPE, class INPUT_TYPE, class OP>
#endif
	static inline void UnaryScatterLoop(const INPUT_TYPE *__restrict idata, AggregateInputData &aggr_input_data,
	                                    STATE_TYPE **__restrict states, const SelectionVector &isel,
	                                    const SelectionVector &ssel, ValidityMask &mask, idx_t count) {
#ifdef DUCKDB_SMALLER_BINARY
		const auto HAS_ISEL = isel.IsSet();
		const auto HAS_SSEL = ssel.IsSet();
#endif
		if (OP::IgnoreNull() && mask.CanHaveNull()) {
			// potential NULL values and NULL values are ignored
			AggregateUnaryInput input(aggr_input_data, mask);
			for (idx_t i = 0; i < count; i++) {
				input.input_idx = HAS_ISEL ? isel.get_index_unsafe(i) : i;
				auto sidx = HAS_SSEL ? ssel.get_index_unsafe(i) : i;
				if (mask.RowIsValidUnsafe(input.input_idx)) {
					OP::template Operation<INPUT_TYPE, STATE_TYPE, OP>(*states[sidx], idata[input.input_idx], input);
				}
			}
		} else {
			// quick path: no NULL values or NULL values are not ignored
			AggregateUnaryInput input(aggr_input_data, mask);
			for (idx_t i = 0; i < count; i++) {
				input.input_idx = HAS_ISEL ? isel.get_index_unsafe(i) : i;
				auto sidx = HAS_SSEL ? ssel.get_index_unsafe(i) : i;
				OP::template Operation<INPUT_TYPE, STATE_TYPE, OP>(*states[sidx], idata[input.input_idx], input);
			}
		}
	}

#ifndef DUCKDB_SMALLER_BINARY
	template <class STATE_TYPE, class INPUT_TYPE, class OP>
	static inline void UnaryFlatUpdateLoop(const INPUT_TYPE *__restrict idata, AggregateInputData &aggr_input_data,
	                                       STATE_TYPE *__restrict state, idx_t count, const ValidityMask &mask) {
		AggregateUnaryInput input(aggr_input_data, mask);
		auto &base_idx = input.input_idx;
		base_idx = 0;
		auto entry_count = ValidityMask::EntryCount(count);
		for (idx_t entry_idx = 0; entry_idx < entry_count; entry_idx++) {
			auto validity_entry = mask.GetValidityEntry(entry_idx);
			idx_t next = MinValue<idx_t>(base_idx + ValidityMask::BITS_PER_VALUE, count);
			if (!OP::IgnoreNull() || ValidityMask::AllValid(validity_entry)) {
				// all valid: perform operation
				for (; base_idx < next; base_idx++) {
					OP::template Operation<INPUT_TYPE, STATE_TYPE, OP>(*state, idata[base_idx], input);
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
						OP::template Operation<INPUT_TYPE, STATE_TYPE, OP>(*state, idata[base_idx], input);
					}
				}
			}
		}
	}
#endif

	template <class STATE_TYPE, class INPUT_TYPE, class OP>
	static inline void UnaryUpdateLoop(const INPUT_TYPE *__restrict idata, AggregateInputData &aggr_input_data,
	                                   STATE_TYPE *__restrict state, idx_t count, ValidityMask &mask,
	                                   const SelectionVector &__restrict sel_vector) {
		AggregateUnaryInput input(aggr_input_data, mask);
		if (OP::IgnoreNull() && mask.CanHaveNull()) {
			// potential NULL values and NULL values are ignored
			for (idx_t i = 0; i < count; i++) {
				input.input_idx = sel_vector.get_index(i);
				if (mask.RowIsValid(input.input_idx)) {
					OP::template Operation<INPUT_TYPE, STATE_TYPE, OP>(*state, idata[input.input_idx], input);
				}
			}
		} else {
			// quick path: no NULL values or NULL values are not ignored
			for (idx_t i = 0; i < count; i++) {
				input.input_idx = sel_vector.get_index(i);
				OP::template Operation<INPUT_TYPE, STATE_TYPE, OP>(*state, idata[input.input_idx], input);
			}
		}
	}

	template <bool CHECK_VALIDITY, class STATE_TYPE, class INPUT_TYPE, class OP>
	static inline void UnarySelectedUpdateLoop(const INPUT_TYPE *__restrict idata,
	                                           AggregateInputData &aggr_input_data, STATE_TYPE *__restrict state,
	                                           idx_t count, const ValidityMask &mask, const sel_t *__restrict sel) {
		AggregateUnaryInput input(aggr_input_data, mask);
		for (idx_t i = 0; i < count; i++) {
			input.input_idx = sel[i];
			if (!CHECK_VALIDITY || mask.RowIsValidUnsafe(input.input_idx)) {
				OP::template Operation<INPUT_TYPE, STATE_TYPE, OP>(*state, idata[input.input_idx], input);
			}
		}
	}

	template <bool CHECK_VALIDITY, class STATE_TYPE, class INPUT_TYPE, class OP>
	static inline void UnarySelectedUpdateLoop(const INPUT_TYPE *__restrict idata,
	                                           AggregateInputData &aggr_input_data, STATE_TYPE *__restrict state,
	                                           idx_t count, const ValidityMask &mask, const SelectionVector &isel,
	                                           const sel_t *__restrict sel) {
		AggregateUnaryInput input(aggr_input_data, mask);
		for (idx_t i = 0; i < count; i++) {
			input.input_idx = isel.get_index(sel[i]);
			if (!CHECK_VALIDITY || mask.RowIsValidUnsafe(input.input_idx)) {
				OP::template Operation<INPUT_TYPE, STATE_TYPE, OP>(*state, idata[input.input_idx], input);
			}
		}
	}

	template <class STATE_TYPE, class INPUT_TYPE, class OP>
	static inline void UnaryUpdateLoop(const INPUT_TYPE *__restrict idata, AggregateInputData &aggr_input_data,
	                                   STATE_TYPE *__restrict state, idx_t count, const ValidityMask &mask,
	                                   const sel_t *__restrict sel) {
		if (OP::IgnoreNull() && mask.CanHaveNull()) {
			UnarySelectedUpdateLoop<true, STATE_TYPE, INPUT_TYPE, OP>(idata, aggr_input_data, state, count, mask, sel);
		} else {
			UnarySelectedUpdateLoop<false, STATE_TYPE, INPUT_TYPE, OP>(idata, aggr_input_data, state, count, mask, sel);
		}
	}

	template <class STATE_TYPE, class A_TYPE, class B_TYPE, class OP>
	static inline void BinaryScatterLoop(const A_TYPE *__restrict adata, AggregateInputData &aggr_input_data,
	                                     const B_TYPE *__restrict bdata, STATE_TYPE **__restrict states, idx_t count,
	                                     const SelectionVector &asel, const SelectionVector &bsel,
	                                     const SelectionVector &ssel, ValidityMask &avalidity,
	                                     ValidityMask &bvalidity) {
		AggregateBinaryInput input(aggr_input_data, avalidity, bvalidity);
		if (OP::IgnoreNull() && (avalidity.CanHaveNull() || bvalidity.CanHaveNull())) {
			// potential NULL values and NULL values are ignored
			for (idx_t i = 0; i < count; i++) {
				input.lidx = asel.get_index(i);
				input.ridx = bsel.get_index(i);
				auto sidx = ssel.get_index(i);
				if (avalidity.RowIsValid(input.lidx) && bvalidity.RowIsValid(input.ridx)) {
					OP::template Operation<A_TYPE, B_TYPE, STATE_TYPE, OP>(*states[sidx], adata[input.lidx],
					                                                       bdata[input.ridx], input);
				}
			}
		} else {
			// quick path: no NULL values or NULL values are not ignored
			for (idx_t i = 0; i < count; i++) {
				input.lidx = asel.get_index(i);
				input.ridx = bsel.get_index(i);
				auto sidx = ssel.get_index(i);
				OP::template Operation<A_TYPE, B_TYPE, STATE_TYPE, OP>(*states[sidx], adata[input.lidx],
				                                                       bdata[input.ridx], input);
			}
		}
	}

	template <class STATE_TYPE, class A_TYPE, class B_TYPE, class OP>
	static inline void BinaryUpdateLoop(const A_TYPE *__restrict adata, AggregateInputData &aggr_input_data,
	                                    const B_TYPE *__restrict bdata, STATE_TYPE *__restrict state, idx_t count,
	                                    const SelectionVector &asel, const SelectionVector &bsel,
	                                    ValidityMask &avalidity, ValidityMask &bvalidity) {
		AggregateBinaryInput input(aggr_input_data, avalidity, bvalidity);
		if (OP::IgnoreNull() && (avalidity.CanHaveNull() || bvalidity.CanHaveNull())) {
			// potential NULL values and NULL values are ignored
			for (idx_t i = 0; i < count; i++) {
				input.lidx = asel.get_index(i);
				input.ridx = bsel.get_index(i);
				if (avalidity.RowIsValid(input.lidx) && bvalidity.RowIsValid(input.ridx)) {
					OP::template Operation<A_TYPE, B_TYPE, STATE_TYPE, OP>(*state, adata[input.lidx], bdata[input.ridx],
					                                                       input);
				}
			}
		} else {
			// quick path: no NULL values or NULL values are not ignored
			for (idx_t i = 0; i < count; i++) {
				input.lidx = asel.get_index(i);
				input.ridx = bsel.get_index(i);
				OP::template Operation<A_TYPE, B_TYPE, STATE_TYPE, OP>(*state, adata[input.lidx], bdata[input.ridx],
				                                                       input);
			}
		}
	}

	template <bool CHECK_VALIDITY, class STATE_TYPE, class A_TYPE, class B_TYPE, class OP>
	static inline void BinarySelectedUpdateLoop(const A_TYPE *__restrict adata, AggregateInputData &aggr_input_data,
	                                            const B_TYPE *__restrict bdata, STATE_TYPE *__restrict state,
	                                            idx_t count, const SelectionVector &asel, const SelectionVector &bsel,
	                                            ValidityMask &avalidity, ValidityMask &bvalidity,
	                                            const sel_t *__restrict sel) {
		AggregateBinaryInput input(aggr_input_data, avalidity, bvalidity);
		for (idx_t i = 0; i < count; i++) {
			const auto idx = sel[i];
			input.lidx = asel.get_index(idx);
			input.ridx = bsel.get_index(idx);
			if (!CHECK_VALIDITY || (avalidity.RowIsValidUnsafe(input.lidx) && bvalidity.RowIsValidUnsafe(input.ridx))) {
				OP::template Operation<A_TYPE, B_TYPE, STATE_TYPE, OP>(*state, adata[input.lidx], bdata[input.ridx],
				                                                       input);
			}
		}
	}

public:
	template <class STATE_TYPE, class OP>
	static void NullaryScatter(Vector &states, AggregateInputData &aggr_input_data, idx_t count) {
		// COUNT(*) can add run lengths directly.
		if (aggr_input_data.clustered) {
			auto &cs = *aggr_input_data.clustered;
			for (idx_t r = 0; r < cs.n_group_runs; r++) {
				OP::template ConstantOperation<STATE_TYPE, OP>(*reinterpret_cast<STATE_TYPE *>(cs.group_runs[r].state),
				                                               aggr_input_data, cs.group_runs[r].count);
			}
			return;
		}
		if (states.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			auto sdata = ConstantVector::GetData<STATE_TYPE *>(states);
			OP::template ConstantOperation<STATE_TYPE, OP>(**sdata, aggr_input_data, count);
#ifndef DUCKDB_SMALLER_BINARY
		} else if (states.GetVectorType() == VectorType::FLAT_VECTOR) {
			auto sdata = FlatVector::GetDataMutable<STATE_TYPE *>(states);
			NullaryFlatLoop<STATE_TYPE, OP>(sdata, aggr_input_data, count);
#endif
		} else {
			UnifiedVectorFormat sdata;
			states.ToUnifiedFormat(count, sdata);
			NullaryScatterLoop<STATE_TYPE, OP>((STATE_TYPE **)sdata.data, aggr_input_data, *sdata.sel, count);
		}
	}

	template <class STATE_TYPE, class OP>
	static void NullaryClusterUpdate(AggregateInputData &aggr_input_data, const ClusteredAggr &clustered, idx_t count) {
		D_ASSERT(aggr_input_data.clustered);
		D_ASSERT(aggr_input_data.clustered.get() == &clustered);
		for (idx_t r = 0; r < clustered.n_group_runs; r++) {
			OP::template ConstantOperation<STATE_TYPE, OP>(*reinterpret_cast<STATE_TYPE *>(clustered.group_runs[r].state),
			                                               aggr_input_data, clustered.group_runs[r].count);
		}
	}

	template <class STATE_TYPE, class OP>
	static void NullaryUpdate(data_ptr_t state, AggregateInputData &aggr_input_data, idx_t count) {
		OP::template ConstantOperation<STATE_TYPE, OP>(*reinterpret_cast<STATE_TYPE *>(state), aggr_input_data, count);
	}

	template <class...>
	using void_t_helper = void;

	template <class OP, class STATE, class = void>
	struct HasClusteredLocalState : std::false_type {};
	template <class OP, class STATE>
	struct HasClusteredLocalState<OP, STATE, void_t_helper<typename OP::template ClusteredLocalState<STATE>::Type>> : std::true_type {};
	template <class OP, class STATE>
	using clustered_local_state_t = typename OP::template ClusteredLocalState<STATE>::Type;

	template <bool DENSE, bool MAPPED>
	struct SelectionIndexer {
		const sel_t *sel;
		const SelectionVector *isel;

		inline idx_t GetIndex(idx_t i) const {
			if constexpr (DENSE) {
				return i;
			} else if constexpr (MAPPED) {
				return isel->get_index(sel[i]);
			} else {
				return sel[i];
			}
		}
	};

	template <bool CHECK_VALIDITY, class LOCAL_TYPE, class INPUT_TYPE, class OP, class INDEXER>
	static inline bool UpdateUnaryClusteredLocalState(const INPUT_TYPE *__restrict vals, LOCAL_TYPE &local,
	                                                  idx_t count, const ValidityMask &mask, INDEXER indexer) {
		bool saw_value = false;
		for (idx_t i = 0; i < count; i++) {
			auto idx = indexer.GetIndex(i);
			if (!CHECK_VALIDITY || mask.RowIsValidUnsafe(idx)) {
				OP::template UpdateClusteredLocal<INPUT_TYPE>(local, vals[idx]);
				saw_value = true;
			}
		}
		return saw_value;
	}

	template <bool CHECK_VALIDITY, class STATE_TYPE, class INPUT_TYPE, class OP, class INDEX_FACTORY>
	static void ExecuteUnaryClusteredLocalState(const INPUT_TYPE *vals, const ClusteredAggr &clustered,
	                                            const ValidityMask &validity, INDEX_FACTORY index_factory) {
		idx_t pos = 0;
		using local_type = clustered_local_state_t<OP, STATE_TYPE>;
		for (idx_t r = 0; r < clustered.n_group_runs; r++) {
			auto &state = *reinterpret_cast<STATE_TYPE *>(clustered.group_runs[r].state);
			local_type local;
			OP::template InitializeClusteredLocal<STATE_TYPE>(local, state);
			auto run_count = clustered.group_runs[r].count;
			auto saw_value = UpdateUnaryClusteredLocalState<CHECK_VALIDITY, local_type, INPUT_TYPE, OP>(
			    vals, local, run_count, validity, index_factory(r, pos));
			OP::template FlushClusteredLocal<STATE_TYPE>(state, local, saw_value);
			pos += run_count;
		}
	}

	template <class STATE_TYPE, class INPUT_TYPE, class OP, class INDEX_FACTORY>
	static void ExecuteUnaryClusteredPrepared(const INPUT_TYPE *vals, const ValidityMask &validity,
	                                          const ClusteredAggr &clustered, INDEX_FACTORY index_factory) {
		if (OP::IgnoreNull() && validity.CanHaveNull()) {
			ExecuteUnaryClusteredLocalState<true, STATE_TYPE, INPUT_TYPE, OP>(vals, clustered, validity, index_factory);
		} else {
			ExecuteUnaryClusteredLocalState<false, STATE_TYPE, INPUT_TYPE, OP>(vals, clustered, validity, index_factory);
		}
	}

	static inline bool IsDenseSingleRun(const ClusteredAggr &clustered, idx_t count) {
		return clustered.n_group_runs == 1 && clustered.group_runs[0].count == count &&
		       clustered.group_runs[0].sel == FlatVector::IncrementalSelectionVector()->data();
	}

	template <bool SIMPLE_DICT, class STATE_TYPE, class INPUT_TYPE, class OP>
	static void ExecuteUnaryClusteredDictionaryLocalState(Vector &input, const ClusteredAggr &clustered, idx_t count,
	                                                      const sel_t *cluster_iter = nullptr) {
		UnifiedVectorFormat idata;
		input.ToUnifiedFormat(count, idata);
		auto vals = UnifiedVectorFormat::GetData<INPUT_TYPE>(idata);
		if constexpr (SIMPLE_DICT) {
			ExecuteUnaryClusteredPrepared<STATE_TYPE, INPUT_TYPE, OP>(
			    vals, idata.validity, clustered,
			    [&](idx_t, idx_t pos) { return SelectionIndexer<false, false> {cluster_iter + pos, nullptr}; });
		} else {
			ExecuteUnaryClusteredPrepared<STATE_TYPE, INPUT_TYPE, OP>(
			    vals, idata.validity, clustered,
			    [&](idx_t r, idx_t) { return SelectionIndexer<false, true> {clustered.group_runs[r].sel, idata.sel}; });
		}
	}

	template <class STATE_TYPE, class INPUT_TYPE, class OP>
	static void ExecuteUnaryClusteredLocalState(Vector &input, const ClusteredAggr &clustered, idx_t count) {
		auto vals = FlatVector::GetData<INPUT_TYPE>(input);
		auto &validity = FlatVector::Validity(input);
		if (IsDenseSingleRun(clustered, count)) {
			auto &state = *reinterpret_cast<STATE_TYPE *>(clustered.group_runs[0].state);
			using local_type = clustered_local_state_t<OP, STATE_TYPE>;
			local_type local;
			OP::template InitializeClusteredLocal<STATE_TYPE>(local, state);
			bool saw_value = false;
			auto dense_indexer = SelectionIndexer<true, false> {nullptr, nullptr};
			if (OP::IgnoreNull() && validity.CanHaveNull()) {
				saw_value = UpdateUnaryClusteredLocalState<true, local_type, INPUT_TYPE, OP>(vals, local, count, 
				                                                                             validity, dense_indexer);
			} else {
				saw_value = UpdateUnaryClusteredLocalState<false, local_type, INPUT_TYPE, OP>(vals, local, count,
				                                                                              validity, dense_indexer);
			}
			OP::template FlushClusteredLocal<STATE_TYPE>(state, local, saw_value);
			return;
		}
		ExecuteUnaryClusteredPrepared<STATE_TYPE, INPUT_TYPE, OP>(
		    vals, validity, clustered,
		    [&](idx_t r, idx_t) { return SelectionIndexer<false, false> {clustered.group_runs[r].sel, nullptr}; });
	}

	// true if OP defines ClusteredOperation (early-exit optimization for first/last/any_value).
	template <class OP, class = void>
	struct HasClusteredOperation : std::false_type {};
	template <class OP>
	struct HasClusteredOperation<OP, void_t_helper<decltype(&OP::template ClusteredOperation<int32_t, int32_t, OP>)>>
	    : std::true_type {};

	template <class STATE_TYPE, class INPUT_TYPE, class OP>
	static void ExecuteUnaryClusteredCustomState(Vector &input, AggregateInputData &aggr_input_data,
	                                             const ClusteredAggr &clustered, idx_t count) {
		static_assert(HasClusteredOperation<OP>::value, "Expected custom clustered operation");
		UnifiedVectorFormat idata;
		input.ToUnifiedFormat(count, idata);
		auto vals = UnifiedVectorFormat::GetData<INPUT_TYPE>(idata);
		AggregateUnaryInput unary_input(aggr_input_data, idata.validity);
		for (idx_t r = 0; r < clustered.n_group_runs; r++) {
			auto &state = *reinterpret_cast<STATE_TYPE *>(clustered.group_runs[r].state);
			OP::template ClusteredOperation<INPUT_TYPE, STATE_TYPE, OP>(state, vals, unary_input,
			                                                            clustered.group_runs[r].sel, *idata.sel,
			                                                            idata.validity, 0,
			                                                            clustered.group_runs[r].count);
		}
	}

	template <class STATE_TYPE, class INPUT_TYPE, class OP>
	static void ExecuteUnaryClustered(Vector &input, AggregateInputData &aggr_input_data, const ClusteredAggr &clustered,
	                                  idx_t count) {
		if constexpr (HasClusteredLocalState<OP, STATE_TYPE>::value) {
			switch (input.GetVectorType()) {
			case VectorType::FLAT_VECTOR:
				ExecuteUnaryClusteredLocalState<STATE_TYPE, INPUT_TYPE, OP>(input, clustered, count);
				return;
			case VectorType::DICTIONARY_VECTOR: {
				auto *cluster_iter = clustered.ClusterIter(input, count);
				if (cluster_iter) {
					ExecuteUnaryClusteredDictionaryLocalState<true, STATE_TYPE, INPUT_TYPE, OP>(input, clustered,
					                                                                            count, cluster_iter);
				} else {
					ExecuteUnaryClusteredDictionaryLocalState<false, STATE_TYPE, INPUT_TYPE, OP>(input, clustered,
					                                                                             count);
				}
				return;
			}
			default:
				break;
			}
		}
		if constexpr (HasClusteredOperation<OP>::value) {
			ExecuteUnaryClusteredCustomState<STATE_TYPE, INPUT_TYPE, OP>(input, aggr_input_data, clustered, count);
		} else {
			D_ASSERT(false);
		}
	}

	template <class STATE_TYPE, class INPUT_TYPE, class OP>
	static void UnaryScatter(Vector &input, Vector &states, AggregateInputData &aggr_input_data, idx_t count) {
		if (aggr_input_data.clustered) {
			ExecuteUnaryClustered<STATE_TYPE, INPUT_TYPE, OP>(input, aggr_input_data, *aggr_input_data.clustered, count);
			return;
		}
		if (input.GetVectorType() == VectorType::CONSTANT_VECTOR &&
		    states.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			if (OP::IgnoreNull() && ConstantVector::IsNull(input)) {
				// constant NULL input in function that ignores NULL values
				return;
			}
			// regular constant: get first state
			auto idata = ConstantVector::GetData<INPUT_TYPE>(input);
			auto sdata = ConstantVector::GetData<STATE_TYPE *>(states);
			AggregateUnaryInput input_data(aggr_input_data, ConstantVector::Validity(input));
			OP::template ConstantOperation<INPUT_TYPE, STATE_TYPE, OP>(**sdata, *idata, input_data, count);
#ifndef DUCKDB_SMALLER_BINARY
		} else if (input.GetVectorType() == VectorType::FLAT_VECTOR &&
		           states.GetVectorType() == VectorType::FLAT_VECTOR) {
			auto idata = FlatVector::GetData<INPUT_TYPE>(input);
			auto sdata = FlatVector::GetDataMutable<STATE_TYPE *>(states);
			UnaryFlatLoop<STATE_TYPE, INPUT_TYPE, OP>(idata, aggr_input_data, sdata, FlatVector::ValidityMutable(input),
			                                          count);
#endif
		} else {
			UnifiedVectorFormat idata, sdata;
			input.ToUnifiedFormat(count, idata);
			states.ToUnifiedFormat(count, sdata);
#ifdef DUCKDB_SMALLER_BINARY
			UnaryScatterLoop<STATE_TYPE, INPUT_TYPE, OP>(UnifiedVectorFormat::GetData<INPUT_TYPE>(idata),
			                                             aggr_input_data, (STATE_TYPE **)sdata.data, *idata.sel,
			                                             *sdata.sel, idata.validity, count);
#else
			if (idata.sel->IsSet()) {
				if (sdata.sel->IsSet()) {
					UnaryScatterLoop<STATE_TYPE, INPUT_TYPE, OP, true, true>(
					    UnifiedVectorFormat::GetData<INPUT_TYPE>(idata), aggr_input_data, (STATE_TYPE **)sdata.data,
					    *idata.sel, *sdata.sel, idata.validity, count);
				} else {
					UnaryScatterLoop<STATE_TYPE, INPUT_TYPE, OP, true, false>(
					    UnifiedVectorFormat::GetData<INPUT_TYPE>(idata), aggr_input_data, (STATE_TYPE **)sdata.data,
					    *idata.sel, *sdata.sel, idata.validity, count);
				}
			} else {
				if (sdata.sel->IsSet()) {
					UnaryScatterLoop<STATE_TYPE, INPUT_TYPE, OP, false, true>(
					    UnifiedVectorFormat::GetData<INPUT_TYPE>(idata), aggr_input_data, (STATE_TYPE **)sdata.data,
					    *idata.sel, *sdata.sel, idata.validity, count);
				} else {
					UnaryScatterLoop<STATE_TYPE, INPUT_TYPE, OP, false, false>(
					    UnifiedVectorFormat::GetData<INPUT_TYPE>(idata), aggr_input_data, (STATE_TYPE **)sdata.data,
					    *idata.sel, *sdata.sel, idata.validity, count);
				}
			}

#endif
		}
	}

	template <class STATE_TYPE, class INPUT_TYPE, class OP>
	static void UnaryUpdate(Vector &input, AggregateInputData &aggr_input_data, data_ptr_t state, idx_t count) {
		switch (input.GetVectorType()) {
		case VectorType::CONSTANT_VECTOR: {
			if (OP::IgnoreNull() && ConstantVector::IsNull(input)) {
				return;
			}
			auto idata = ConstantVector::GetData<INPUT_TYPE>(input);
			AggregateUnaryInput input_data(aggr_input_data, ConstantVector::Validity(input));
			OP::template ConstantOperation<INPUT_TYPE, STATE_TYPE, OP>(*reinterpret_cast<STATE_TYPE *>(state), *idata,
			                                                           input_data, count);
			break;
		}
#ifndef DUCKDB_SMALLER_BINARY
		case VectorType::FLAT_VECTOR: {
			auto idata = FlatVector::GetData<INPUT_TYPE>(input);
			UnaryFlatUpdateLoop<STATE_TYPE, INPUT_TYPE, OP>(idata, aggr_input_data, (STATE_TYPE *)state, count,
			                                                FlatVector::ValidityMutable(input));
			break;
		}
#endif
		default: {
			UnifiedVectorFormat idata;
			input.ToUnifiedFormat(count, idata);
			UnaryUpdateLoop<STATE_TYPE, INPUT_TYPE, OP>(UnifiedVectorFormat::GetData<INPUT_TYPE>(idata),
			                                            aggr_input_data, (STATE_TYPE *)state, count, idata.validity,
			                                            *idata.sel);
			break;
		}
		}
	}

	template <class STATE_TYPE, class INPUT_TYPE, class OP>
	static void UnaryClusterUpdate(Vector &input, AggregateInputData &aggr_input_data, const ClusteredAggr &clustered,
	                               idx_t count) {
		D_ASSERT(aggr_input_data.clustered);
		D_ASSERT(aggr_input_data.clustered.get() == &clustered);
		ExecuteUnaryClustered<STATE_TYPE, INPUT_TYPE, OP>(input, aggr_input_data, clustered, count);
	}

	template <class STATE_TYPE, class A_TYPE, class B_TYPE, class OP>
	static void BinaryScatter(AggregateInputData &aggr_input_data, Vector &a, Vector &b, Vector &states, idx_t count) {
		UnifiedVectorFormat adata, bdata, sdata;

		a.ToUnifiedFormat(count, adata);
		b.ToUnifiedFormat(count, bdata);
		states.ToUnifiedFormat(count, sdata);

		BinaryScatterLoop<STATE_TYPE, A_TYPE, B_TYPE, OP>(
		    UnifiedVectorFormat::GetData<A_TYPE>(adata), aggr_input_data, UnifiedVectorFormat::GetData<B_TYPE>(bdata),
		    (STATE_TYPE **)sdata.data, count, *adata.sel, *bdata.sel, *sdata.sel, adata.validity, bdata.validity);
	}

	template <class STATE_TYPE, class A_TYPE, class B_TYPE, class OP>
	static void BinaryUpdate(AggregateInputData &aggr_input_data, Vector &a, Vector &b, data_ptr_t state, idx_t count) {
		UnifiedVectorFormat adata, bdata;

		a.ToUnifiedFormat(count, adata);
		b.ToUnifiedFormat(count, bdata);

		BinaryUpdateLoop<STATE_TYPE, A_TYPE, B_TYPE, OP>(
		    UnifiedVectorFormat::GetData<A_TYPE>(adata), aggr_input_data, UnifiedVectorFormat::GetData<B_TYPE>(bdata),
		    (STATE_TYPE *)state, count, *adata.sel, *bdata.sel, adata.validity, bdata.validity);
	}

	template <class STATE_TYPE, class OP>
	static void Combine(Vector &source, Vector &target, AggregateInputData &aggr_input_data, idx_t count) {
		D_ASSERT(source.GetType().id() == LogicalTypeId::POINTER && target.GetType().id() == LogicalTypeId::POINTER);
		auto sdata = source.Values<const STATE_TYPE *>(count);
		auto tdata = target.Values<STATE_TYPE *>(count);

		for (idx_t i = 0; i < count; i++) {
			OP::template Combine<STATE_TYPE, OP>(*sdata[i].GetValueUnsafe(), *tdata[i].GetValueUnsafe(),
			                                     aggr_input_data);
		}
	}

	template <class STATE_TYPE, class RESULT_TYPE, class OP>
	static void Finalize(Vector &states, AggregateInputData &aggr_input_data, Vector &result, idx_t count,
	                     idx_t offset) {
		if (states.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);

			auto sdata = ConstantVector::GetData<STATE_TYPE *>(states);
			auto rdata = ConstantVector::GetData<RESULT_TYPE>(result);
			AggregateFinalizeData finalize_data(result, aggr_input_data);
			OP::template Finalize<RESULT_TYPE, STATE_TYPE>(**sdata, *rdata, finalize_data);
		} else {
			D_ASSERT(states.GetVectorType() == VectorType::FLAT_VECTOR);
			result.SetVectorType(VectorType::FLAT_VECTOR);

			auto sdata = FlatVector::GetData<STATE_TYPE *>(states);
			auto rdata = FlatVector::GetDataMutable<RESULT_TYPE>(result);
			AggregateFinalizeData finalize_data(result, aggr_input_data);
			for (idx_t i = 0; i < count; i++) {
				finalize_data.result_idx = i + offset;
				OP::template Finalize<RESULT_TYPE, STATE_TYPE>(*sdata[i], rdata[finalize_data.result_idx],
				                                               finalize_data);
			}
		}
	}

	template <class STATE_TYPE, class OP>
	static void VoidFinalize(Vector &states, AggregateInputData &aggr_input_data, Vector &result, idx_t count,
	                         idx_t offset) {
		if (states.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);

			auto sdata = ConstantVector::GetData<STATE_TYPE *>(states);
			AggregateFinalizeData finalize_data(result, aggr_input_data);
			OP::template Finalize<STATE_TYPE>(**sdata, finalize_data);
		} else {
			D_ASSERT(states.GetVectorType() == VectorType::FLAT_VECTOR);
			result.SetVectorType(VectorType::FLAT_VECTOR);

			auto sdata = FlatVector::GetData<STATE_TYPE *>(states);
			AggregateFinalizeData finalize_data(result, aggr_input_data);
			for (idx_t i = 0; i < count; i++) {
				finalize_data.result_idx = i + offset;
				OP::template Finalize<STATE_TYPE>(*sdata[i], finalize_data);
			}
		}
	}

	template <typename OP>
	static void IntersectFrames(const SubFrames &lefts, const SubFrames &rights, OP &op) {
		const auto cover_start = MinValue(rights[0].start, lefts[0].start);
		const auto cover_end = MaxValue(rights.back().end, lefts.back().end);
		const FrameBounds last(cover_end, cover_end);

		//	Subframe indices
		idx_t l = 0;
		idx_t r = 0;
		for (auto i = cover_start; i < cover_end;) {
			uint8_t overlap = 0;

			// Are we in the previous frame?
			auto left = &last;
			if (l < lefts.size()) {
				left = &lefts[l];
				overlap |= uint8_t(left->start <= i && i < left->end) << 0;
			}

			// Are we in the current frame?
			auto right = &last;
			if (r < rights.size()) {
				right = &rights[r];
				overlap |= uint8_t(right->start <= i && i < right->end) << 1;
			}

			auto limit = i;
			switch (overlap) {
			case 0x00:
				// i ∉ F U P
				limit = MinValue(right->start, left->start);
				op.Neither(i, limit);
				break;
			case 0x01:
				// i ∈ P \ F
				limit = MinValue(left->end, right->start);
				op.Left(i, limit);
				break;
			case 0x02:
				// i ∈ F \ P
				limit = MinValue(right->end, left->start);
				op.Right(i, limit);
				break;
			case 0x03:
			default:
				D_ASSERT(overlap == 0x03);
				// i ∈ F ∩ P
				limit = MinValue(right->end, left->end);
				op.Both(i, limit);
				break;
			}

			// Advance  the subframe indices
			i = limit;
			l += (i == left->end);
			r += (i == right->end);
		}
	}

	template <class STATE_TYPE, class OP>
	static void Destroy(Vector &states, AggregateInputData &aggr_input_data, idx_t count) {
		auto sdata = states.Values<STATE_TYPE *>(count);
		;
		for (idx_t i = 0; i < count; i++) {
			OP::template Destroy<STATE_TYPE>(*sdata[i].GetValueUnsafe(), aggr_input_data);
		}
	}
};

} // namespace duckdb
