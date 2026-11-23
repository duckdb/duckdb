#include "duckdb/execution/operator/order/physical_top_n.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/common/atomic.hpp"

#include "duckdb/common/mutex.hpp"
#include "duckdb/common/arena_containers/arena_vector.hpp"
#include "duckdb/common/sorting/topn_heap.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/create_sort_key.hpp"
#include "duckdb/storage/data_table.hpp"

namespace duckdb {

PhysicalTopN::PhysicalTopN(PhysicalPlan &physical_plan, ClientContext &context, vector<LogicalType> types,
                           vector<BoundOrderByNode> orders, idx_t limit, idx_t offset,
                           shared_ptr<DynamicFilterData> dynamic_filter_p, idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::TOP_N, std::move(types), estimated_cardinality),
      orders(std::move(orders)), limit(limit), offset(offset), dynamic_filter(std::move(dynamic_filter_p)) {
	const auto order_count = this->orders.size();
	for (idx_t order_idx = 0; order_idx < order_count; order_idx++) {
		auto &type = this->orders[order_idx].expression->GetReturnType();
		not_distinct_from.push_back(BoundComparisonExpression::CreateOnColumns(
		    context, ExpressionType::COMPARE_NOT_DISTINCT_FROM, type, order_idx, type, order_count + order_idx));
	}
}

PhysicalTopN::~PhysicalTopN() {
}

class TopNGlobalSinkState : public GlobalSinkState {
public:
	TopNGlobalSinkState(ClientContext &context, const PhysicalTopN &op)
	    : heap(context, op.types, op.orders, op.not_distinct_from, op.limit, op.offset),
	      boundary_value(op.orders[0], op.dynamic_filter) {
	}

	mutex lock;
	TopNHeap heap;
	TopNBoundaryValue boundary_value;
};

class TopNLocalSinkState : public LocalSinkState {
public:
	TopNLocalSinkState(ExecutionContext &context, const vector<LogicalType> &payload_types,
	                   const vector<BoundOrderByNode> &orders, const vector<unique_ptr<Expression>> &not_distinct_from,
	                   idx_t limit, idx_t offset)
	    : heap(context, payload_types, orders, not_distinct_from, limit, offset) {
	}

	TopNHeap heap;
};

unique_ptr<LocalSinkState> PhysicalTopN::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<TopNLocalSinkState>(context, types, orders, not_distinct_from, limit, offset);
}

unique_ptr<GlobalSinkState> PhysicalTopN::GetGlobalSinkState(ClientContext &context) const {
	if (dynamic_filter) {
		dynamic_filter->Reset();
	}
	return make_uniq<TopNGlobalSinkState>(context, *this);
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
SinkResultType PhysicalTopN::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	// append to the local sink state
	auto &gstate = input.global_state.Cast<TopNGlobalSinkState>();
	auto &sink = input.local_state.Cast<TopNLocalSinkState>();
	sink.heap.Sink(chunk, &gstate.boundary_value);
	sink.heap.Reduce();
	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Combine
//===--------------------------------------------------------------------===//
SinkCombineResultType PhysicalTopN::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<TopNGlobalSinkState>();
	auto &lstate = input.local_state.Cast<TopNLocalSinkState>();

	// scan the local top N and append it to the global heap
	lstate.heap.Finalize();
	lock_guard<mutex> guard(gstate.lock);
	gstate.heap.Combine(lstate.heap);

	return SinkCombineResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
SinkFinalizeType PhysicalTopN::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                        OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<TopNGlobalSinkState>();
	// global finalize: compute the final top N
	gstate.heap.Finalize();
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class TopNLocalSourceState : public LocalSourceState {
public:
	idx_t pos = DConstants::INVALID_INDEX;
	idx_t end = DConstants::INVALID_INDEX;
	idx_t batch_index = DConstants::INVALID_INDEX;
};

class TopNGlobalSourceState : public GlobalSourceState {
public:
	explicit TopNGlobalSourceState(TopNGlobalSinkState &sink_p) : sink(sink_p), batch_index(0) {
		sink.heap.InitializeScan(state, true);
		total_rows = state.scan_order.size() - MinValue<idx_t>(sink.heap.offset, state.scan_order.size());
	}

	idx_t MaxThreads() override {
		return MaxValue<idx_t>(sink.heap.heap.size() / TUPLES_PER_BATCH, 1);
	}

public:
	static constexpr idx_t CHUNKS_PER_BATCH = 60;
	static constexpr idx_t TUPLES_PER_BATCH = CHUNKS_PER_BATCH * STANDARD_VECTOR_SIZE;

	TopNGlobalSinkState &sink;
	TopNScanState state;
	idx_t batch_index;
	idx_t total_rows;
	atomic<idx_t> rows_scanned {0};
};

unique_ptr<GlobalSourceState> PhysicalTopN::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<TopNGlobalSourceState>(this->sink_state->Cast<TopNGlobalSinkState>());
}

ProgressData PhysicalTopN::GetProgress(ClientContext &context, GlobalSourceState &gstate) const {
	auto &state = gstate.Cast<TopNGlobalSourceState>();
	if (state.total_rows == 0) {
		return ProgressData {1.0, 1.0, false};
	}
	return ProgressData {double(state.rows_scanned.load()), double(state.total_rows), false};
}

unique_ptr<LocalSourceState> PhysicalTopN::GetLocalSourceState(ExecutionContext &context,
                                                               GlobalSourceState &gstate) const {
	return make_uniq<TopNLocalSourceState>();
}

SourceResultType PhysicalTopN::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                               OperatorSourceInput &input) const {
	if (limit == 0) {
		return SourceResultType::FINISHED;
	}
	auto &sink = sink_state->Cast<TopNGlobalSinkState>();
	auto &gstate = input.global_state.Cast<TopNGlobalSourceState>();
	auto &lstate = input.local_state.Cast<TopNLocalSourceState>();

	if (lstate.pos == lstate.end) {
		// Obtain new scan indices from the global state
		annotated_lock_guard<annotated_mutex> guard(gstate.lock);
		lstate.pos = gstate.state.pos;
		gstate.state.pos += TopNGlobalSourceState::TUPLES_PER_BATCH;
		lstate.end = gstate.state.pos;
		lstate.batch_index = gstate.batch_index++;
	}

	sink.heap.Scan(gstate.state, chunk, lstate.pos);
	gstate.rows_scanned.fetch_add(chunk.size(), std::memory_order_relaxed);

	return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
}

OperatorPartitionData PhysicalTopN::GetPartitionData(ExecutionContext &context, DataChunk &chunk,
                                                     GlobalSourceState &gstate_p, LocalSourceState &lstate_p,
                                                     const OperatorPartitionInfo &partition_info) const {
	if (partition_info.RequiresPartitionColumns()) {
		throw InternalException("PhysicalOrder::GetPartitionData: partition columns not supported");
	}
	auto &lstate = lstate_p.Cast<TopNLocalSourceState>();
	return OperatorPartitionData(lstate.batch_index);
}

InsertionOrderPreservingMap<string> PhysicalTopN::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Top"] = to_string(limit);
	if (offset > 0) {
		result["Offset"] = to_string(offset);
	}

	string orders_info;
	for (idx_t i = 0; i < orders.size(); i++) {
		if (i > 0) {
			orders_info += "\n";
		}
		orders_info += orders[i].expression->ToString() + " ";
		orders_info += orders[i].type == OrderType::DESCENDING ? "DESC" : "ASC";
	}
	result["Order By"] = orders_info;
	SetEstimatedCardinality(result, estimated_cardinality);
	return result;
}

} // namespace duckdb
