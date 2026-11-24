#include "duckdb/common/sorting/topn_sort.hpp"
#include "duckdb/common/sorting/topn_heap.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/sorting/sorted_run.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// TopNSortGlobalSinkState
//===--------------------------------------------------------------------===//
class TopNSortGlobalSinkState : public GlobalSinkState {
public:
	TopNSortGlobalSinkState(ClientContext &client, const TopNSort &topn);

	ProgressData GetSinkProgress(ClientContext &context, const ProgressData source) const {
		// there is nothing to sort
		return source;
	}

	//! System and query state
	ClientContext &client;
	mutable mutex lock;
	TopNHeap heap;
	shared_ptr<DynamicFilterData> dynamic_filter;
	TopNBoundaryValue boundary_value;

	// Threading
	atomic<idx_t> count;
	//! Keeps the sink progress monotonic
	MonotonicProgress sink_progress;
};

TopNSortGlobalSinkState::TopNSortGlobalSinkState(ClientContext &client, const TopNSort &topn)
    : client(client),
      heap(client, topn.payload_types, topn.orders, topn.not_distinct_from, topn.partition_limit.GetIndex(), 0),
      boundary_value(topn.orders[0], dynamic_filter), count(0) {
}

//===--------------------------------------------------------------------===//
// TopNSortLocalSinkState
//===--------------------------------------------------------------------===//
class TopNSortLocalSinkState : public LocalSinkState {
public:
	using LocalSortStatePtr = unique_ptr<LocalSinkState>;

	TopNSortLocalSinkState(ExecutionContext &context, const TopNSort &topn);

	//! Global state
	const TopNSort &topn;

	TopNHeap heap;

	//! Shared expression evaluation
	ExpressionExecutor sort_exec;
	DataChunk group_chunk;
	DataChunk sort_chunk;
	DataChunk payload_chunk;
};

TopNSortLocalSinkState::TopNSortLocalSinkState(ExecutionContext &context, const TopNSort &topn)
    : topn(topn),
      heap(context, topn.payload_types, topn.orders, topn.not_distinct_from, topn.partition_limit.GetIndex(), 0),
      sort_exec(context.client) {
	vector<LogicalType> sort_types;
	for (const auto &expr : topn.sort_exprs) {
		sort_types.emplace_back(expr->GetReturnType());
		sort_exec.AddExpression(*expr);
	}
	sort_chunk.Initialize(context.client, sort_types);

	auto payload_types = topn.payload_types;
	payload_chunk.Initialize(context.client, payload_types);
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
SinkResultType TopNSort::Sink(ExecutionContext &context, DataChunk &input_chunk, OperatorSinkInput &sink) const {
	auto &gstate = sink.global_state.Cast<TopNSortGlobalSinkState>();
	auto &lstate = sink.local_state.Cast<TopNSortLocalSinkState>();
	gstate.count += input_chunk.size();

	//	Payload prefix is the input data
	auto &payload_chunk = lstate.payload_chunk;
	payload_chunk.Reset();
	for (column_t i = 0; i < input_chunk.ColumnCount(); ++i) {
		payload_chunk.data[i].Reference(input_chunk.data[i]);
	}

	//	Compute any sort columns that are not references and append them to the end of the payload
	auto &sort_chunk = lstate.sort_chunk;
	auto &sort_exec = lstate.sort_exec;
	if (!sort_exprs.empty()) {
		sort_chunk.Reset();
		sort_exec.Execute(input_chunk, sort_chunk);
		for (column_t i = 0; i < sort_chunk.ColumnCount(); ++i) {
			payload_chunk.data[input_chunk.ColumnCount() + i].Reference(sort_chunk.data[i]);
		}
	}

	//	OVER(ORDER BY...)
	lstate.heap.Sink(payload_chunk, &gstate.boundary_value);
	lstate.heap.Reduce();

	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Combine
//===--------------------------------------------------------------------===//
SinkCombineResultType TopNSort::Combine(ExecutionContext &context, OperatorSinkCombineInput &combine) const {
	auto &gstate = combine.global_state.Cast<TopNSortGlobalSinkState>();
	auto &lstate = combine.local_state.Cast<TopNSortLocalSinkState>();

	//	OVER(ORDER BY...)
	// scan the local top N and append it to the global heap
	lstate.heap.Finalize();
	lock_guard<mutex> guard(gstate.lock);
	gstate.heap.Combine(lstate.heap);

	return SinkCombineResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
SinkFinalizeType TopNSort::Finalize(ClientContext &client, OperatorSinkFinalizeInput &finalize) const {
	auto &gsink = finalize.global_state.Cast<TopNSortGlobalSinkState>();

	//	Did we get any data?
	if (!gsink.count) {
		return SinkFinalizeType::NO_OUTPUT_POSSIBLE;
	}

	//	OVER(ORDER BY...)
	gsink.heap.Finalize();

	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// GetSinkProgress
//===--------------------------------------------------------------------===//
ProgressData TopNSort::GetSinkProgress(ClientContext &client, GlobalSinkState &gstate,
                                       const ProgressData source) const {
	auto &gsink = gstate.Cast<TopNSortGlobalSinkState>();
	return gsink.GetSinkProgress(client, source);
}

//===--------------------------------------------------------------------===//
// TopNSortGlobalSourceState
//===--------------------------------------------------------------------===//
class TopNSortGlobalSourceState : public GlobalSourceState {
public:
	using ChunkRow = TopNSort::ChunkRow;
	using ChunkRows = TopNSort::ChunkRows;

	TopNSortGlobalSourceState(ClientContext &client, TopNSortGlobalSinkState &gsink);

	TopNSortGlobalSinkState &gsink;
	TopNScanState scan;
	ChunkRows chunk_rows;

	//	Parallel scan and build
	static constexpr idx_t CHUNKS_PER_BATCH = 60;
	static constexpr idx_t TUPLES_PER_BATCH = CHUNKS_PER_BATCH * STANDARD_VECTOR_SIZE;

	vector<unique_ptr<ColumnDataCollection>> batches;
	idx_t total_rows;
	atomic<idx_t> rows_scanned {0};
};

TopNSortGlobalSourceState::TopNSortGlobalSourceState(ClientContext &client, TopNSortGlobalSinkState &gsink)
    : gsink(gsink) {
	if (!gsink.count) {
		return;
	}

	gsink.heap.InitializeScan(scan, true);
	total_rows = scan.scan_order.size();

	//	One sorted group
	ChunkRow chunk_row;
	chunk_row.count = total_rows;
	chunk_row.chunks = (chunk_row.count + STANDARD_VECTOR_SIZE - 1) / STANDARD_VECTOR_SIZE;

	chunk_rows.emplace_back(chunk_row);
}

//===--------------------------------------------------------------------===//
// TopNSort
//===--------------------------------------------------------------------===//
bool TopNSort::CanOptimize(optional_idx limit, idx_t cardinality) {
	if (!limit.IsValid() || !cardinality) {
		return false;
	}

	return TopNHeap::CanOptimize(limit.GetIndex(), cardinality);
}

TopNSort::TopNSort(ClientContext &client, const vector<BoundOrderByNode> &order_bys, const Types &input_types,
                   optional_idx partition_limit)
    : SortStrategy(input_types, partition_limit) {
	//	We have to compute ordering expressions ourselves and materialise them.
	//	To do this, we scan the orders and add generate extra payload columns that we can reference.
	for (const auto &order : order_bys) {
		orders.emplace_back(order.Copy());
	}

	for (auto &order : orders) {
		auto &expr = *order.expression;
		if (expr.GetExpressionClass() == ExpressionClass::BOUND_REF) {
			auto &ref = expr.Cast<BoundReferenceExpression>();
			sort_ids.emplace_back(ref.Index());
			continue;
		}

		//	Real expression - replace with a ref and save the expression
		auto saved = std::move(order.expression);
		const auto type = saved->GetReturnType();
		const auto idx = payload_types.size();
		order.expression = make_uniq<BoundReferenceExpression>(type, idx);
		sort_ids.emplace_back(idx);
		payload_types.emplace_back(type);
		sort_exprs.emplace_back(std::move(saved));
	}

	const auto order_count = orders.size();
	for (idx_t order_idx = 0; order_idx < order_count; order_idx++) {
		auto &type = orders[order_idx].expression->GetReturnType();
		not_distinct_from.push_back(BoundComparisonExpression::CreateOnColumns(
		    client, ExpressionType::COMPARE_NOT_DISTINCT_FROM, type, order_idx, type, order_count + order_idx));
	}
}

unique_ptr<GlobalSinkState> TopNSort::GetGlobalSinkState(ClientContext &client) const {
	return make_uniq<TopNSortGlobalSinkState>(client, *this);
}

unique_ptr<LocalSinkState> TopNSort::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<TopNSortLocalSinkState>(context, *this);
}

unique_ptr<GlobalSourceState> TopNSort::GetGlobalSourceState(ClientContext &client, GlobalSinkState &sink) const {
	return make_uniq<TopNSortGlobalSourceState>(client, sink.Cast<TopNSortGlobalSinkState>());
}

const TopNSort::ChunkRows &TopNSort::GetHashGroups(GlobalSourceState &gstate) const {
	auto &gsource = gstate.Cast<TopNSortGlobalSourceState>();
	return gsource.chunk_rows;
}

SourceResultType TopNSort::MaterializeColumnData(ExecutionContext &execution, idx_t hash_bin,
                                                 OperatorSourceInput &source) const {
	auto &gsource = source.global_state.Cast<TopNSortGlobalSourceState>();
	auto &gsink = gsource.gsink;

	//	Build a range into a PartitionedColumnData
	idx_t pos = DConstants::INVALID_INDEX;
	idx_t end = DConstants::INVALID_INDEX;
	optional_ptr<ColumnDataCollection> batch_data;

	DataChunk chunk;
	chunk.Initialize(execution.client, payload_types);
	ColumnDataAppendState append;
	for (;;) {
		if (pos == end) {
			// Obtain new scan indices from the global state
			annotated_lock_guard<annotated_mutex> guard(gsource.lock);
			pos = gsource.scan.pos;
			gsource.scan.pos += TopNSortGlobalSourceState::TUPLES_PER_BATCH;
			end = gsource.scan.pos;
			auto batch = make_uniq<ColumnDataCollection>(execution.client, payload_types);
			batch->InitializeAppend(append);
			batch_data = batch.get();
			gsource.batches.emplace_back(std::move(batch));
		}

		chunk.Reset();
		gsink.heap.Scan(gsource.scan, chunk, pos);
		if (chunk.size() == 0) {
			break;
		}
		gsource.rows_scanned.fetch_add(chunk.size(), std::memory_order_relaxed);
		batch_data->Append(append, chunk);
	}

	return SourceResultType::FINISHED;
}

TopNSort::HashGroupPtr TopNSort::GetColumnData(idx_t hash_bin, OperatorSourceInput &source) const {
	auto &gsource = source.global_state.Cast<TopNSortGlobalSourceState>();

	D_ASSERT(hash_bin == 0);

	annotated_lock_guard<annotated_mutex> guard(gsource.lock);
	unique_ptr<ColumnDataCollection> result;
	for (auto &batch : gsource.batches) {
		if (!result) {
			result = std::move(batch);
		} else {
			result->Combine(*batch);
		}
	}

	if (result && result->Count() == gsource.rows_scanned.load()) {
		return result;
	}

	return nullptr;
}

SourceResultType TopNSort::MaterializeSortedRun(ExecutionContext &context, idx_t hash_bin,
                                                OperatorSourceInput &source) const {
	return MaterializeColumnData(context, hash_bin, source);
}

TopNSort::SortedRunPtr TopNSort::GetSortedRun(ClientContext &client, idx_t hash_bin,
                                              OperatorSourceInput &source) const {
	throw InternalException("TopNSort::GetSortedRun is not supported");
}

} // namespace duckdb
