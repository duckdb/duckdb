#include "duckdb/execution/operator/order/physical_order.hpp"

#include "duckdb/common/sort/sort.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/optimizer/cascade/search/CGroupExpression.h"
#include "duckdb/parallel/base_pipeline_event.hpp"
#include "duckdb/parallel/event.hpp"
#include "duckdb/storage/buffer_manager.hpp"

#include <cstddef>

namespace duckdb {

PhysicalOrder::PhysicalOrder(vector<LogicalType> types, vector<BoundOrderByNode> orders, vector<idx_t> projections,
                             idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::ORDER_BY, std::move(types), estimated_cardinality),
      orders(std::move(orders)), projections(std::move(projections)) {
}

CEnfdOrder::EPropEnforcingType PhysicalOrder::EpetOrder(CExpressionHandle &exprhdl,
                                                        vector<BoundOrderByNode> &peo) const {
	bool compactible = true;
	for (size_t ul = 0; ul < peo.size(); ul++) {
		bool flag = false;
		for (size_t sul = 0; sul < orders.size(); sul++) {
			if (orders[sul].Equals(peo[ul])) {
				flag = true;
				break;
			}
		}
		if (!flag) {
			compactible = false;
			break;
		}
	}
	if (compactible) {
		// required order is already established by sort operator
		return CEnfdOrder::EpetUnnecessary;
	}
	// required order is incompatible with the order established by the
	// sort operator, prohibit adding another sort operator on top
	return CEnfdOrder::EpetProhibited;
}

COrderSpec *PhysicalOrder::PosRequired(CExpressionHandle &exprhdl, COrderSpec *pos_required, ULONG child_index,
                                       vector<CDrvdProp *> pdrgpdp_ctxt, ULONG ul_opt_req) const {
	// sort operator is order-establishing and does not require child to deliver
	// any sort order; we return an empty sort order as child requirement
	return new COrderSpec();
}

bool PhysicalOrder::FProvidesReqdCols(CExpressionHandle &exprhdl, vector<ColumnBinding> pcrs_required,
                                      ULONG ul_opt_req) const {
	return FUnaryProvidesReqdCols(exprhdl, pcrs_required);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSort::PcrsRequired
//
//	@doc:
//		Compute required columns of the n-th child;
//
//---------------------------------------------------------------------------
vector<ColumnBinding> PhysicalOrder::PcrsRequired(CExpressionHandle &exprhdl, vector<ColumnBinding> pcrs_required,
                                                  ULONG child_index, vector<CDrvdProp *> pdrgpdp_ctxt, ULONG ul_opt_req) {
	vector<ColumnBinding> pcrs_sort;
	for (auto &child : orders) {
		vector<ColumnBinding> cell = child.expression->getColumnBinding();
		pcrs_sort.insert(pcrs_sort.end(), cell.begin(), cell.begin());
	}
	vector<ColumnBinding> pcrs;
	std::set_union(pcrs_sort.begin(), pcrs_sort.end(), pcrs_required.begin(), pcrs_required.end(), pcrs.begin());
	vector<ColumnBinding> pcrs_child_reqd = PcrsChildReqd(exprhdl, pcrs, child_index);
	return pcrs_child_reqd;
}

CKeyCollection *DeriveKeyCollection(CExpressionHandle &exprhdl) {
	return nullptr;
}

Operator *PhysicalOrder::SelfRehydrate(CCostContext *pcc, duckdb::vector<Operator *> pdrgpexpr,
                                       CDrvdPropCtxtPlan *pdpctxtplan) {
	CGroupExpression *pgexpr = pcc->m_group_expression;
	double cost = pcc->m_cost;
	ULONG arity = pgexpr->Arity();
	vector<double> pdrgpcost;
	for (ULONG ul = 0; ul < arity; ul++) {
		double cost_child = pdrgpexpr[ul]->m_cost;
		pdrgpcost.push_back(cost_child);
	}
	cost = pcc->CostCompute(pdrgpcost);
	vector<BoundOrderByNode> v_orders;
	for(auto &child : this->orders) {
		v_orders.emplace_back(child.Copy());
	}
	PhysicalOrder *pexpr = new PhysicalOrder(types, std::move(v_orders), projections, 0);
	pexpr->m_cost = cost;
	pexpr->m_group_expression = pgexpr;
	return pexpr;
}

unique_ptr<Operator> PhysicalOrder::Copy() {
	/* PhysicalOrder fields */
	vector<BoundOrderByNode> v_orders;
	for(auto &child : this->orders) {
		v_orders.emplace_back(child.Copy());
	}
	unique_ptr<PhysicalOrder> copy = make_uniq<PhysicalOrder>(this->types, std::move(v_orders), this->projections, 0);
	
	/* PhysicalOperator fields */
	copy->m_total_opt_requests = this->m_total_opt_requests;
	
	/* Operator fields */
	copy->m_derived_property_relation = this->m_derived_property_relation;
	copy->m_derived_property_plan = this->m_derived_property_plan;
	copy->m_required_plan_property = this->m_required_plan_property;
	if (nullptr != this->estimated_props) {
		copy->estimated_props = this->estimated_props->Copy();
	}
	copy->types = this->types;
	copy->estimated_cardinality = this->estimated_cardinality;
	copy->has_estimated_cardinality = this->has_estimated_cardinality;
	for(auto &child : this->children) {
		copy->AddChild(child->Copy());
	}
	copy->m_group_expression = this->m_group_expression;
	copy->m_cost = this->m_cost;
	return copy;
}

unique_ptr<Operator> PhysicalOrder::CopyWithNewGroupExpression(CGroupExpression* pgexpr) {
	/* PhysicalOrder fields */
	vector<BoundOrderByNode> v_orders;
	for(auto &child : this->orders) {
		v_orders.emplace_back(child.Copy());
	}
	unique_ptr<PhysicalOrder> copy = make_uniq<PhysicalOrder>(this->types, std::move(v_orders), this->projections, 0);
	
	/* PhysicalOperator fields */
	copy->m_total_opt_requests = this->m_total_opt_requests;
	
	/* Operator fields */
	copy->m_derived_property_relation = this->m_derived_property_relation;
	copy->m_derived_property_plan = this->m_derived_property_plan;
	copy->m_required_plan_property = this->m_required_plan_property;
	if (nullptr != this->estimated_props) {
		copy->estimated_props = this->estimated_props->Copy();
	}
	copy->types = this->types;
	copy->estimated_cardinality = this->estimated_cardinality;
	copy->has_estimated_cardinality = this->has_estimated_cardinality;
	for(auto &child : this->children) {
		copy->AddChild(child->Copy());
	}
	copy->m_group_expression = pgexpr;
	copy->m_cost = this->m_cost;
	return copy;
}

unique_ptr<Operator> PhysicalOrder::CopyWithNewChildren(CGroupExpression* pgexpr,
															vector<unique_ptr<Operator>> pdrgpexpr,
															double cost) {
	/* PhysicalOrder fields */
	vector<BoundOrderByNode> v_orders;
	for(auto &child : this->orders) {
		v_orders.emplace_back(child.Copy());
	}
	unique_ptr<PhysicalOrder> copy = make_uniq<PhysicalOrder>(this->types, std::move(v_orders), this->projections, 0);
	
	/* PhysicalOperator fields */
	copy->m_total_opt_requests = this->m_total_opt_requests;
	
	/* Operator fields */
	copy->m_derived_property_relation = this->m_derived_property_relation;
	copy->m_derived_property_plan = this->m_derived_property_plan;
	copy->m_required_plan_property = this->m_required_plan_property;
	if (nullptr != this->estimated_props) {
		copy->estimated_props = this->estimated_props->Copy();
	}
	copy->types = this->types;
	copy->estimated_cardinality = this->estimated_cardinality;
	copy->has_estimated_cardinality = this->has_estimated_cardinality;
	for(auto &child : pdrgpexpr) {
		copy->AddChild(std::move(child));
	}
	copy->m_group_expression = pgexpr;
	copy->m_cost = cost;
	return copy;
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class OrderGlobalSinkState : public GlobalSinkState {
public:
	OrderGlobalSinkState(BufferManager &buffer_manager, const PhysicalOrder &order, RowLayout &payload_layout)
	    : global_sort_state(buffer_manager, order.orders, payload_layout) {
	}

	//! Global sort state
	GlobalSortState global_sort_state;
	//! Memory usage per thread
	idx_t memory_per_thread;
};

class OrderLocalSinkState : public LocalSinkState {
public:
	OrderLocalSinkState(ClientContext &context, const PhysicalOrder &op) : key_executor(context) {
		// Initialize order clause expression executor and DataChunk
		vector<LogicalType> key_types;
		for (auto &order : op.orders) {
			key_types.push_back(order.expression->return_type);
			key_executor.AddExpression(*order.expression);
		}
		auto &allocator = Allocator::Get(context);
		keys.Initialize(allocator, key_types);
		payload.Initialize(allocator, op.types);
	}

public:
	//! The local sort state
	LocalSortState local_sort_state;
	//! Key expression executor, and chunk to hold the vectors
	ExpressionExecutor key_executor;
	DataChunk keys;
	//! Payload chunk to hold the vectors
	DataChunk payload;
};

unique_ptr<GlobalSinkState> PhysicalOrder::GetGlobalSinkState(ClientContext &context) const {
	// Get the payload layout from the return types
	RowLayout payload_layout;
	payload_layout.Initialize(types);
	auto state = make_uniq<OrderGlobalSinkState>(BufferManager::GetBufferManager(context), *this, payload_layout);
	// Set external (can be force with the PRAGMA)
	state->global_sort_state.external = ClientConfig::GetConfig(context).force_external;
	state->memory_per_thread = GetMaxThreadMemory(context);
	return std::move(state);
}

unique_ptr<LocalSinkState> PhysicalOrder::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<OrderLocalSinkState>(context.client, *this);
}

SinkResultType PhysicalOrder::Sink(ExecutionContext &context, GlobalSinkState &gstate_p, LocalSinkState &lstate_p,
                                   DataChunk &input) const {
	auto &gstate = gstate_p.Cast<OrderGlobalSinkState>();
	auto &lstate = lstate_p.Cast<OrderLocalSinkState>();

	auto &global_sort_state = gstate.global_sort_state;
	auto &local_sort_state = lstate.local_sort_state;

	// Initialize local state (if necessary)
	if (!local_sort_state.initialized) {
		local_sort_state.Initialize(global_sort_state, BufferManager::GetBufferManager(context.client));
	}

	// Obtain sorting columns
	auto &keys = lstate.keys;
	keys.Reset();
	lstate.key_executor.Execute(input, keys);

	auto &payload = lstate.payload;
	payload.ReferenceColumns(input, projections);

	// Sink the data into the local sort state
	keys.Verify();
	input.Verify();
	local_sort_state.SinkChunk(keys, payload);

	// When sorting data reaches a certain size, we sort it
	if (local_sort_state.SizeInBytes() >= gstate.memory_per_thread) {
		local_sort_state.Sort(global_sort_state, true);
	}
	return SinkResultType::NEED_MORE_INPUT;
}

void PhysicalOrder::Combine(ExecutionContext &context, GlobalSinkState &gstate_p, LocalSinkState &lstate_p) const {
	auto &gstate = gstate_p.Cast<OrderGlobalSinkState>();
	auto &lstate = lstate_p.Cast<OrderLocalSinkState>();
	gstate.global_sort_state.AddLocalState(lstate.local_sort_state);
}

class PhysicalOrderMergeTask : public ExecutorTask {
public:
	PhysicalOrderMergeTask(shared_ptr<Event> event_p, ClientContext &context, OrderGlobalSinkState &state)
	    : ExecutorTask(context), event(std::move(event_p)), context(context), state(state) {
	}

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
		// Initialize merge sorted and iterate until done
		auto &global_sort_state = state.global_sort_state;
		MergeSorter merge_sorter(global_sort_state, BufferManager::GetBufferManager(context));
		merge_sorter.PerformInMergeRound();
		event->FinishTask();
		return TaskExecutionResult::TASK_FINISHED;
	}

private:
	shared_ptr<Event> event;
	ClientContext &context;
	OrderGlobalSinkState &state;
};

class OrderMergeEvent : public BasePipelineEvent {
public:
	OrderMergeEvent(OrderGlobalSinkState &gstate_p, Pipeline &pipeline_p)
	    : BasePipelineEvent(pipeline_p), gstate(gstate_p) {
	}

	OrderGlobalSinkState &gstate;

public:
	void Schedule() override {
		auto &context = pipeline->GetClientContext();
		// Schedule tasks equal to the number of threads, which will each merge multiple partitions
		auto &ts = TaskScheduler::GetScheduler(context);
		idx_t num_threads = ts.NumberOfThreads();
		vector<unique_ptr<Task>> merge_tasks;
		for (idx_t tnum = 0; tnum < num_threads; tnum++) {
			merge_tasks.push_back(make_uniq<PhysicalOrderMergeTask>(shared_from_this(), context, gstate));
		}
		SetTasks(std::move(merge_tasks));
	}

	void FinishEvent() override {
		auto &global_sort_state = gstate.global_sort_state;

		global_sort_state.CompleteMergeRound();
		if (global_sort_state.sorted_blocks.size() > 1) {
			// Multiple blocks remaining: Schedule the next round
			PhysicalOrder::ScheduleMergeTasks(*pipeline, *this, gstate);
		}
	}
};

SinkFinalizeType PhysicalOrder::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                         GlobalSinkState &gstate_p) const {
	auto &state = gstate_p.Cast<OrderGlobalSinkState>();
	auto &global_sort_state = state.global_sort_state;

	if (global_sort_state.sorted_blocks.empty()) {
		// Empty input!
		return SinkFinalizeType::NO_OUTPUT_POSSIBLE;
	}

	// Prepare for merge sort phase
	global_sort_state.PrepareMergePhase();

	// Start the merge phase or finish if a merge is not necessary
	if (global_sort_state.sorted_blocks.size() > 1) {
		PhysicalOrder::ScheduleMergeTasks(pipeline, event, state);
	}
	return SinkFinalizeType::READY;
}

void PhysicalOrder::ScheduleMergeTasks(Pipeline &pipeline, Event &event, OrderGlobalSinkState &state) {
	// Initialize global sort state for a round of merging
	state.global_sort_state.InitializeMergeRound();
	auto new_event = make_shared<OrderMergeEvent>(state, pipeline);
	event.InsertEvent(std::move(new_event));
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class PhysicalOrderGlobalSourceState : public GlobalSourceState {
public:
	explicit PhysicalOrderGlobalSourceState(OrderGlobalSinkState &sink) : next_batch_index(0) {
		auto &global_sort_state = sink.global_sort_state;
		if (global_sort_state.sorted_blocks.empty()) {
			total_batches = 0;
		} else {
			D_ASSERT(global_sort_state.sorted_blocks.size() == 1);
			total_batches = global_sort_state.sorted_blocks[0]->payload_data->data_blocks.size();
		}
	}

	idx_t MaxThreads() override {
		return total_batches;
	}

public:
	atomic<idx_t> next_batch_index;
	idx_t total_batches;
};

unique_ptr<GlobalSourceState> PhysicalOrder::GetGlobalSourceState(ClientContext &context) const {
	auto &sink = this->sink_state->Cast<OrderGlobalSinkState>();
	return make_uniq<PhysicalOrderGlobalSourceState>(sink);
}

class PhysicalOrderLocalSourceState : public LocalSourceState {
public:
	explicit PhysicalOrderLocalSourceState(PhysicalOrderGlobalSourceState &gstate)
	    : batch_index(gstate.next_batch_index++) {
	}

public:
	idx_t batch_index;
	unique_ptr<PayloadScanner> scanner;
};

unique_ptr<LocalSourceState> PhysicalOrder::GetLocalSourceState(ExecutionContext &context,
                                                                GlobalSourceState &gstate_p) const {
	auto &gstate = gstate_p.Cast<PhysicalOrderGlobalSourceState>();
	return make_uniq<PhysicalOrderLocalSourceState>(gstate);
}

void PhysicalOrder::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate_p,
                            LocalSourceState &lstate_p) const {
	auto &gstate = gstate_p.Cast<PhysicalOrderGlobalSourceState>();
	auto &lstate = lstate_p.Cast<PhysicalOrderLocalSourceState>();

	if (lstate.scanner && lstate.scanner->Remaining() == 0) {
		lstate.batch_index = gstate.next_batch_index++;
		lstate.scanner = nullptr;
	}

	if (lstate.batch_index >= gstate.total_batches) {
		return;
	}

	if (!lstate.scanner) {
		auto &sink = this->sink_state->Cast<OrderGlobalSinkState>();
		auto &global_sort_state = sink.global_sort_state;
		lstate.scanner = make_uniq<PayloadScanner>(global_sort_state, lstate.batch_index, true);
	}

	lstate.scanner->Scan(chunk);
}

idx_t PhysicalOrder::GetBatchIndex(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate_p,
                                   LocalSourceState &lstate_p) const {
	auto &lstate = lstate_p.Cast<PhysicalOrderLocalSourceState>();
	return lstate.batch_index;
}

string PhysicalOrder::ParamsToString() const {
	string result = "ORDERS:\n";
	for (idx_t i = 0; i < orders.size(); i++) {
		if (i > 0) {
			result += "\n";
		}
		result += orders[i].expression->ToString() + " ";
		result += orders[i].type == OrderType::DESCENDING ? "DESC" : "ASC";
	}
	return result;
}

} // namespace duckdb
