#include "duckdb/execution/operator/order/physical_order.hpp"

#include "duckdb/common/sorting/sort.hpp"

namespace duckdb {

PhysicalOrder::PhysicalOrder(PhysicalPlan &physical_plan, vector<LogicalType> types, vector<BoundOrderByNode> orders,
                             vector<idx_t> projections, idx_t estimated_cardinality, bool is_index_sort_p)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::ORDER_BY, std::move(types), estimated_cardinality),
      orders(std::move(orders)), projections(std::move(projections)), is_index_sort(is_index_sort_p) {
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class OrderGlobalSinkState : public GlobalSinkState {
public:
	OrderGlobalSinkState(const PhysicalOrder &op, ClientContext &context)
	    : sort(context, op.orders, op.children[0].get().types, op.projections, op.is_index_sort),
	      state(sort.GetGlobalSinkState(context)) {
	}

public:
	Sort sort;
	unique_ptr<GlobalSinkState> state;
};

class OrderLocalSinkState : public LocalSinkState {
public:
	OrderLocalSinkState() {
	}

public:
	unique_ptr<LocalSinkState> state;
};

unique_ptr<LocalSinkState> PhysicalOrder::GetLocalSinkState(ExecutionContext &) const {
	return make_uniq<OrderLocalSinkState>();
}

unique_ptr<GlobalSinkState> PhysicalOrder::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<OrderGlobalSinkState>(*this, context);
}

SinkResultType PhysicalOrder::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<OrderGlobalSinkState>();
	auto &lstate = input.local_state.Cast<OrderLocalSinkState>();
	if (!lstate.state) {
		lstate.state = gstate.sort.GetLocalSinkState(context);
	}
	OperatorSinkInput sort_input {*gstate.state, *lstate.state, input.interrupt_state};
	return gstate.sort.Sink(context, chunk, sort_input);
}

SinkCombineResultType PhysicalOrder::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<OrderGlobalSinkState>();
	auto &lstate = input.local_state.Cast<OrderLocalSinkState>();
	if (!lstate.state) {
		return SinkCombineResultType::FINISHED;
	}
	OperatorSinkCombineInput sort_input {*gstate.state, *lstate.state, input.interrupt_state};
	return gstate.sort.Combine(context, sort_input);
}

SinkFinalizeType PhysicalOrder::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                         OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<OrderGlobalSinkState>();
	OperatorSinkFinalizeInput sort_input {*gstate.state, input.interrupt_state};
	return gstate.sort.Finalize(context, sort_input);
}

ProgressData PhysicalOrder::GetSinkProgress(ClientContext &context, GlobalSinkState &gstate_p,
                                            const ProgressData source_progress) const {
	auto &gstate = gstate_p.Cast<OrderGlobalSinkState>();
	return gstate.sort.GetSinkProgress(context, *gstate.state, source_progress);
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class OrderGlobalSourceState : public GlobalSourceState {
public:
	explicit OrderGlobalSourceState(ClientContext &context, OrderGlobalSinkState &sink)
	    : sort(sink.sort), state(sort.GetGlobalSourceState(context, *sink.state)) {
	}

public:
	idx_t MaxThreads() override {
		return state->MaxThreads();
	}

public:
	Sort &sort;
	unique_ptr<GlobalSourceState> state;
};

class OrderLocalSourceState : public LocalSourceState {
public:
	explicit OrderLocalSourceState(ExecutionContext &context, OrderGlobalSourceState &gstate)
	    : state(gstate.sort.GetLocalSourceState(context, *gstate.state)) {
	}

public:
	unique_ptr<LocalSourceState> state;
};

unique_ptr<LocalSourceState> PhysicalOrder::GetLocalSourceState(ExecutionContext &context,
                                                                GlobalSourceState &gstate_p) const {
	auto &gstate = gstate_p.Cast<OrderGlobalSourceState>();
	return make_uniq<OrderLocalSourceState>(context, gstate);
}

unique_ptr<GlobalSourceState> PhysicalOrder::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<OrderGlobalSourceState>(context, sink_state->Cast<OrderGlobalSinkState>());
}

SourceResultType PhysicalOrder::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                OperatorSourceInput &input) const {
	auto &gstate = input.global_state.Cast<OrderGlobalSourceState>();
	auto &lstate = input.local_state.Cast<OrderLocalSourceState>();
	OperatorSourceInput sort_input {*gstate.state, *lstate.state, input.interrupt_state};
	return gstate.sort.GetData(context, chunk, sort_input);
}

OperatorPartitionData PhysicalOrder::GetPartitionData(ExecutionContext &context, DataChunk &chunk,
                                                      GlobalSourceState &gstate_p, LocalSourceState &lstate_p,
                                                      const OperatorPartitionInfo &partition_info) const {
	auto &gstate = gstate_p.Cast<OrderGlobalSourceState>();
	auto &lstate = lstate_p.Cast<OrderLocalSourceState>();
	if (partition_info.RequiresPartitionColumns()) {
		throw InternalException("PhysicalOrder::GetPartitionData: partition columns not supported");
	}
	return gstate.sort.GetPartitionData(context, chunk, *gstate.state, *lstate.state, partition_info);
}

ProgressData PhysicalOrder::GetProgress(ClientContext &context, GlobalSourceState &gstate_p) const {
	auto &gstate = gstate_p.Cast<OrderGlobalSourceState>();
	return gstate.sort.GetProgress(context, *gstate.state);
}

InsertionOrderPreservingMap<string> PhysicalOrder::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	string orders_info;
	for (idx_t i = 0; i < orders.size(); i++) {
		if (i > 0) {
			orders_info += "\n";
		}
		orders_info += orders[i].expression->ToString() + " ";
		orders_info += orders[i].type == OrderType::DESCENDING ? "DESC" : "ASC";
	}
	result["__order_by__"] = orders_info;
	return result;
}

} // namespace duckdb
