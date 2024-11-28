#include "duckdb/function/window/window_naive_aggregator.hpp"
#include "duckdb/function/window/window_collection.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// WindowNaiveAggregator
//===--------------------------------------------------------------------===//
WindowNaiveAggregator::WindowNaiveAggregator(const BoundWindowExpression &wexpr, const WindowExcludeMode exclude_mode,
                                             WindowSharedExpressions &shared)
    : WindowAggregator(wexpr, exclude_mode, shared) {
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
}

void WindowNaiveState::Finalize(WindowAggregatorGlobalState &gastate, CollectionPtr collection) {
	WindowAggregatorLocalState::Finalize(gastate, collection);

	//	Set up the comparison scanner just in case
	if (!comparer) {
		comparer = make_uniq<WindowCursor>(*collection, gastate.aggregator.child_idx);
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

	if (leaves.ColumnCount() == 0 && !types.empty()) {
		leaves.Initialize(Allocator::DefaultAllocator(), types);
	}

	auto fdata = FlatVector::GetData<data_ptr_t>(statef);
	auto pdata = FlatVector::GetData<data_ptr_t>(statep);

	HashRow hash_row(*this);
	EqualRow equal_row(*this);
	RowSet row_set(STANDARD_VECTOR_SIZE, hash_row, equal_row);

	WindowAggregator::EvaluateSubFrames(bounds, aggregator.exclude_mode, count, row_idx, frames, [&](idx_t rid) {
		auto agg_state = fdata[rid];
		aggr.function.initialize(aggr.function, agg_state);

		//	Just update the aggregate with the unfiltered input rows
		row_set.clear();
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
