#include "duckdb/function/window/window_naive_aggregator.hpp"
#include "duckdb/common/sort/sort.hpp"
#include "duckdb/function/window/window_collection.hpp"
#include "duckdb/function/window/window_shared_expressions.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/function/window/window_aggregate_function.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// WindowNaiveAggregator
//===--------------------------------------------------------------------===//
WindowNaiveAggregator::WindowNaiveAggregator(const WindowAggregateExecutor &executor, WindowSharedExpressions &shared)
    : WindowAggregator(executor.wexpr, shared), executor(executor) {

	for (const auto &order : wexpr.arg_orders) {
		arg_order_idx.emplace_back(shared.RegisterCollection(order.expression, false));
	}
}

WindowNaiveAggregator::~WindowNaiveAggregator() {
}

class WindowNaiveState : public WindowAggregatorLocalState {
public:
	struct HashRow {
		explicit HashRow(WindowNaiveState &state) : state(state) {
		}

		inline size_t operator()(const idx_t &i) const {
			return state.Hash(i);
		}

		WindowNaiveState &state;
	};

	struct EqualRow {
		explicit EqualRow(WindowNaiveState &state) : state(state) {
		}

		inline bool operator()(const idx_t &lhs, const idx_t &rhs) const {
			return state.KeyEqual(lhs, rhs);
		}

		WindowNaiveState &state;
	};

	using RowSet = std::unordered_set<idx_t, HashRow, EqualRow>;

	explicit WindowNaiveState(const WindowNaiveAggregator &gsink);

	void Finalize(WindowAggregatorGlobalState &gastate, CollectionPtr collection) override;

	void Evaluate(const WindowAggregatorGlobalState &gsink, const DataChunk &bounds, Vector &result, idx_t count,
	              idx_t row_idx);

protected:
	//! Flush the accumulated intermediate states into the result states
	void FlushStates(const WindowAggregatorGlobalState &gsink);

	//! Hashes a value for the hash table
	size_t Hash(idx_t rid);
	//! Compares two values for the hash table
	bool KeyEqual(const idx_t &lhs, const idx_t &rhs);

	//! The global state
	const WindowNaiveAggregator &aggregator;
	//! Data pointer that contains a vector of states, used for row aggregation
	vector<data_t> state;
	//! Reused result state container for the aggregate
	Vector statef;
	//! A vector of pointers to "state", used for buffering intermediate aggregates
	Vector statep;
	//! Input data chunk, used for leaf segment aggregation
	DataChunk leaves;
	//! The rows beging updated.
	SelectionVector update_sel;
	//! Count of buffered values
	idx_t flush_count;
	//! The frame boundaries, used for EXCLUDE
	SubFrames frames;
	//! The optional hash table used for DISTINCT
	Vector hashes;
	//! The state used for comparing the collection across chunk boundaries
	unique_ptr<WindowCursor> comparer;

	//! The state used for scanning ORDER BY values from the collection
	unique_ptr<WindowCursor> arg_orderer;
	//! Reusable sort key chunk
	DataChunk orderby_sort;
	//! Reusable sort payload chunk
	DataChunk orderby_payload;
	//! Reusable sort key slicer
	SelectionVector orderby_sel;
	//! Reusable payload layout.
	RowLayout payload_layout;
};

WindowNaiveState::WindowNaiveState(const WindowNaiveAggregator &aggregator_p)
    : aggregator(aggregator_p), state(aggregator.state_size * STANDARD_VECTOR_SIZE), statef(LogicalType::POINTER),
      statep((LogicalType::POINTER)), flush_count(0), hashes(LogicalType::HASH) {
	InitSubFrames(frames, aggregator.exclude_mode);

	update_sel.Initialize();

	//	Build the finalise vector that just points to the result states
	data_ptr_t state_ptr = state.data();
	D_ASSERT(statef.GetVectorType() == VectorType::FLAT_VECTOR);
	statef.SetVectorType(VectorType::CONSTANT_VECTOR);
	statef.Flatten(STANDARD_VECTOR_SIZE);
	auto fdata = FlatVector::GetData<data_ptr_t>(statef);
	for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; ++i) {
		fdata[i] = state_ptr;
		state_ptr += aggregator.state_size;
	}

	//	Initialise any ORDER BY data
	if (!aggregator.arg_order_idx.empty() && !arg_orderer) {
		orderby_payload.Initialize(Allocator::DefaultAllocator(), {LogicalType::UBIGINT});
		payload_layout.Initialize(orderby_payload.GetTypes());
		orderby_sel.Initialize();
	}
}

void WindowNaiveState::Finalize(WindowAggregatorGlobalState &gastate, CollectionPtr collection) {
	WindowAggregatorLocalState::Finalize(gastate, collection);

	//	Set up the comparison scanner just in case
	if (!comparer) {
		comparer = make_uniq<WindowCursor>(*collection, aggregator.child_idx);
	}

	//	Set up the argument ORDER BY scanner if needed
	if (!aggregator.arg_order_idx.empty() && !arg_orderer) {
		arg_orderer = make_uniq<WindowCursor>(*collection, aggregator.arg_order_idx);
		orderby_sort.Initialize(BufferAllocator::Get(gastate.context), arg_orderer->chunk.GetTypes());
	}

	// Initialise the chunks
	const auto types = cursor->chunk.GetTypes();
	if (leaves.ColumnCount() == 0 && !types.empty()) {
		leaves.Initialize(BufferAllocator::Get(gastate.context), types);
	}
}

void WindowNaiveState::FlushStates(const WindowAggregatorGlobalState &gsink) {
	if (!flush_count) {
		return;
	}

	auto &scanned = cursor->chunk;
	leaves.Slice(scanned, update_sel, flush_count);

	const auto &aggr = gsink.aggr;
	AggregateInputData aggr_input_data(aggr.GetFunctionData(), allocator);
	aggr.function.update(leaves.data.data(), aggr_input_data, leaves.ColumnCount(), statep, flush_count);

	flush_count = 0;
}

size_t WindowNaiveState::Hash(idx_t rid) {
	D_ASSERT(cursor->RowIsVisible(rid));
	auto s = cursor->RowOffset(rid);
	auto &scanned = cursor->chunk;
	SelectionVector sel(&s);
	leaves.Slice(scanned, sel, 1);
	leaves.Hash(hashes);

	return *FlatVector::GetData<hash_t>(hashes);
}

bool WindowNaiveState::KeyEqual(const idx_t &lidx, const idx_t &ridx) {
	//	One of the indices will be scanned, so make it the left one
	auto lhs = lidx;
	auto rhs = ridx;
	if (!cursor->RowIsVisible(lhs)) {
		std::swap(lhs, rhs);
		D_ASSERT(cursor->RowIsVisible(lhs));
	}

	auto &scanned = cursor->chunk;
	auto l = cursor->RowOffset(lhs);
	SelectionVector lsel(&l);

	auto rreader = cursor.get();
	if (!cursor->RowIsVisible(rhs)) {
		//	Values on different pages!
		rreader = comparer.get();
		rreader->Seek(rhs);
	}
	auto rscanned = &rreader->chunk;
	auto r = rreader->RowOffset(rhs);
	SelectionVector rsel(&r);

	sel_t f = 0;
	SelectionVector fsel(&f);

	for (column_t c = 0; c < scanned.ColumnCount(); ++c) {
		Vector left(scanned.data[c], lsel, 1);
		Vector right(rscanned->data[c], rsel, 1);
		if (!VectorOperations::NotDistinctFrom(left, right, nullptr, 1, nullptr, &fsel)) {
			return false;
		}
	}

	return true;
}

void WindowNaiveState::Evaluate(const WindowAggregatorGlobalState &gsink, const DataChunk &bounds, Vector &result,
                                idx_t count, idx_t row_idx) {
	const auto &aggr = gsink.aggr;
	auto &filter_mask = gsink.filter_mask;
	const auto types = cursor->chunk.GetTypes();

	auto fdata = FlatVector::GetData<data_ptr_t>(statef);
	auto pdata = FlatVector::GetData<data_ptr_t>(statep);

	HashRow hash_row(*this);
	EqualRow equal_row(*this);
	RowSet row_set(STANDARD_VECTOR_SIZE, hash_row, equal_row);

	WindowAggregator::EvaluateSubFrames(bounds, aggregator.exclude_mode, count, row_idx, frames, [&](idx_t rid) {
		auto agg_state = fdata[rid];
		aggr.function.initialize(aggr.function, agg_state);

		//	Reset the DISTINCT hash table
		row_set.clear();

		// 	Sort the input rows by the argument
		if (arg_orderer) {
			auto &context = aggregator.executor.context;
			auto &orders = aggregator.wexpr.arg_orders;
			GlobalSortState global_sort(context, orders, payload_layout);
			LocalSortState local_sort;
			local_sort.Initialize(global_sort, global_sort.buffer_manager);

			idx_t orderby_count = 0;
			auto orderby_row = FlatVector::GetData<idx_t>(orderby_payload.data[0]);
			for (const auto &frame : frames) {
				for (auto f = frame.start; f < frame.end; ++f) {
					//	FILTER before the ORDER BY
					if (!filter_mask.RowIsValid(f)) {
						continue;
					}

					if (!arg_orderer->RowIsVisible(f) || orderby_count >= STANDARD_VECTOR_SIZE) {
						if (orderby_count) {
							orderby_sort.Reference(arg_orderer->chunk);
							orderby_sort.Slice(orderby_sel, orderby_count);
							orderby_payload.SetCardinality(orderby_count);
							local_sort.SinkChunk(orderby_sort, orderby_payload);
						}
						orderby_count = 0;
						arg_orderer->Seek(f);
					}
					orderby_row[orderby_count] = f;
					orderby_sel.set_index(orderby_count++, arg_orderer->RowOffset(f));
				}
			}
			if (orderby_count) {
				orderby_sort.Reference(arg_orderer->chunk);
				orderby_sort.Slice(orderby_sel, orderby_count);
				orderby_payload.SetCardinality(orderby_count);
				local_sort.SinkChunk(orderby_sort, orderby_payload);
			}

			global_sort.AddLocalState(local_sort);
			if (global_sort.sorted_blocks.empty()) {
				return;
			}
			global_sort.PrepareMergePhase();
			while (global_sort.sorted_blocks.size() > 1) {
				global_sort.InitializeMergeRound();
				MergeSorter merge_sorter(global_sort, global_sort.buffer_manager);
				merge_sorter.PerformInMergeRound();
				global_sort.CompleteMergeRound(false);
			}

			PayloadScanner scanner(global_sort);
			while (scanner.Remaining()) {
				orderby_payload.Reset();
				scanner.Scan(orderby_payload);
				orderby_row = FlatVector::GetData<idx_t>(orderby_payload.data[0]);
				for (idx_t i = 0; i < orderby_payload.size(); ++i) {
					const auto f = orderby_row[i];
					//	Seek to the current position
					if (!cursor->RowIsVisible(f)) {
						//	We need to flush when we cross a chunk boundary
						FlushStates(gsink);
						cursor->Seek(f);
					}

					//	Filter out duplicates
					if (aggr.IsDistinct() && !row_set.insert(f).second) {
						continue;
					}

					pdata[flush_count] = agg_state;
					update_sel[flush_count++] = cursor->RowOffset(f);
					if (flush_count >= STANDARD_VECTOR_SIZE) {
						FlushStates(gsink);
					}
				}
			}
			return;
		}

		//	Just update the aggregate with the unfiltered input rows
		for (const auto &frame : frames) {
			for (auto f = frame.start; f < frame.end; ++f) {
				if (!filter_mask.RowIsValid(f)) {
					continue;
				}

				//	Seek to the current position
				if (!cursor->RowIsVisible(f)) {
					//	We need to flush when we cross a chunk boundary
					FlushStates(gsink);
					cursor->Seek(f);
				}

				//	Filter out duplicates
				if (aggr.IsDistinct() && !row_set.insert(f).second) {
					continue;
				}

				pdata[flush_count] = agg_state;
				update_sel[flush_count++] = cursor->RowOffset(f);
				if (flush_count >= STANDARD_VECTOR_SIZE) {
					FlushStates(gsink);
				}
			}
		}
	});

	//	Flush the final states
	FlushStates(gsink);

	//	Finalise the result aggregates and write to the result
	AggregateInputData aggr_input_data(aggr.GetFunctionData(), allocator);
	aggr.function.finalize(statef, aggr_input_data, result, count, 0);

	//	Destruct the result aggregates
	if (aggr.function.destructor) {
		aggr.function.destructor(statef, aggr_input_data, count);
	}
}

unique_ptr<WindowAggregatorState> WindowNaiveAggregator::GetLocalState(const WindowAggregatorState &gstate) const {
	return make_uniq<WindowNaiveState>(*this);
}

void WindowNaiveAggregator::Evaluate(const WindowAggregatorState &gsink, WindowAggregatorState &lstate,
                                     const DataChunk &bounds, Vector &result, idx_t count, idx_t row_idx) const {
	const auto &gnstate = gsink.Cast<WindowAggregatorGlobalState>();
	auto &lnstate = lstate.Cast<WindowNaiveState>();
	lnstate.Evaluate(gnstate, bounds, result, count, row_idx);
}

} // namespace duckdb
